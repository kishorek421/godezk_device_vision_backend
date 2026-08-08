require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { createClient } = require('redis');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3010;
const BACKDOOR_BASE_URL = (process.env.BACKDOOR_BASE_URL || '').replace(/\/$/, '');
const BACKDOOR_TOKEN = process.env.BACKDOOR_TOKEN || null;
const ORG_ID = process.env.ORG_ID || 'default';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const MAX_EVENTS = parseInt(process.env.MAX_EVENTS || '100000', 10);
const FRAMES_PER_DEPLOYMENT = parseInt(process.env.FRAMES_PER_DEPLOYMENT || '10', 10);
const ENABLE_MOCK = process.env.ENABLE_MOCK === 'true';

const COMPONENTS = [
  'rtsp_camera', 'rtsp_handler', 'function_adapter', 'device_connection_manager', 'frame_weir',
  'frame_bus', 'queuer', 'pre_screener', 'perception_gate', 'inference_service', 'semantic_event',
  'redis_cooldown', 'postgresql_workflow_runner_queue', 'queue_handler', 'redis_executor_task_channel',
  'executor_worker_pool', 'postgresql_task_hydration', 'runner_execute_graph', 'graph_nodes',
  'postgresql_execution_record', 'redis_telemetry_events'
];

const STAGE_TO_COMPONENT = {
  frame_received: 'queuer',
  inference_start: 'inference_service',
  inference_done: 'inference_service',
  gate_decision: 'perception_gate',
  workflow_queued: 'postgresql_workflow_runner_queue',
  workflow_started: 'runner_execute_graph',
  workflow_done: 'runner_execute_graph',
  execution_record: 'postgresql_execution_record'
};

const eventStore = [];
let mockSeeded = false;

const app = express();
app.use(cors());
app.use(express.json());

function nowMs() { return Date.now(); }

function clampEvents() {
  if (eventStore.length > MAX_EVENTS) eventStore.splice(0, eventStore.length - MAX_EVENTS);
}

function parseTime(value) {
  if (value == null) return null;
  const str = String(value).trim();
  const n = Number(str);
  if (!Number.isNaN(n)) return n;
  const m = str.match(/^(\d+(?:\.\d+)?)\s*([smhd])$/i);
  if (m) {
    const v = parseFloat(m[1]);
    const mult = { s: 1000, m: 60 * 1000, h: 60 * 60 * 1000, d: 24 * 60 * 60 * 1000 };
    return Math.round(v * mult[m[2].toLowerCase()]);
  }
  return new Date(str).getTime() || null;
}

function backdoorHeaders(orgId) {
  const headers = { 'x-org-id': orgId || ORG_ID };
  if (BACKDOOR_TOKEN) headers.Authorization = `Bearer ${BACKDOOR_TOKEN}`;
  return headers;
}

async function callBackdoor(pathname, params = {}, orgId) {
  if (!BACKDOOR_BASE_URL) return null;
  try {
    const { data } = await axios.get(`${BACKDOOR_BASE_URL}${pathname}`, {
      params, headers: backdoorHeaders(orgId), timeout: 5000
    });
    return data;
  } catch (_) { return null; }
}

// ── Deployments ────────────────────────────────────────────────

async function fetchRunningDeployments(orgId) {
  const data = await callBackdoor('/api/workflows/deployments', { org_id: orgId || ORG_ID }, orgId);
  if (data && Array.isArray(data.deployments)) {
    return data.deployments.filter(d => {
      const s = String(d.status || '').toLowerCase();
      return s === 'active' || s === 'running' || (d.active_listeners || 0) > 0;
    });
  }
  if (ENABLE_MOCK) {
    seedMockData();
    return mockDeployments();
  }
  return buildDeploymentsFromEvents();
}

function buildDeploymentsFromEvents() {
  const byDep = new Map();
  for (const ev of eventStore) {
    if (!ev.deployment_id) continue;
    const dep = byDep.get(ev.deployment_id) || {
      id: ev.deployment_id,
      org_id: ev.org_id || ORG_ID,
      workflow_name: `Deployment ${String(ev.deployment_id).slice(-8)}`,
      status: 'active',
      active_listeners: 1,
      device_ids: new Set(),
      event_types: new Set()
    };
    if (ev.device_id) dep.device_ids.add(String(ev.device_id));
    if (ev.event_type) dep.event_types.add(ev.event_type);
    byDep.set(ev.deployment_id, dep);
  }
  return Array.from(byDep.values()).map(d => ({
    ...d,
    device_ids: Array.from(d.device_ids),
    workflow_name: d.workflow_name + (d.event_types.size ? ` (${Array.from(d.event_types).join(', ')})` : '')
  }));
}

// ── Frames: group pipeline timing events per device ────────────
// A frame starts at a `frame_received` event; all events for the same
// device until the next `frame_received` belong to that frame.

function buildFrames(events) {
  const byDevice = {};
  const sorted = [...events].filter(e => e && e.ts != null).sort((a, b) => a.ts - b.ts);
  for (const ev of sorted) {
    const dev = ev.device_id || 'unknown';
    if (!byDevice[dev]) byDevice[dev] = [];
    if (ev.stage === 'frame_received' || byDevice[dev].length === 0) {
      byDevice[dev].push([ev]);
    } else {
      byDevice[dev][byDevice[dev].length - 1].push(ev);
    }
  }

  const frames = [];
  for (const [deviceId, frameGroups] of Object.entries(byDevice)) {
    for (const group of frameGroups) {
      const first = group[0];
      const last = group[group.length - 1];
      const breakdown = {};
      let deploymentId = null;
      let eventType = null;
      let decision = null;
      for (const ev of group) {
        if (ev.deployment_id) deploymentId = ev.deployment_id;
        if (ev.event_type) eventType = ev.event_type;
        if (ev.stage === 'gate_decision' && ev.decision) decision = ev.decision;
        const comp = STAGE_TO_COMPONENT[ev.stage] || ev.component;
        if (comp && ev.duration_ms != null) {
          breakdown[comp] = (breakdown[comp] || 0) + Number(ev.duration_ms);
        }
      }
      const totalMs = last.ts - first.ts + (Number(last.duration_ms) || 0);
      frames.push({
        frame_id: `${deviceId}-${first.ts}`,
        device_id: deviceId,
        deployment_id: deploymentId,
        event_type: eventType,
        decision,
        started_at: new Date(first.ts).toISOString(),
        started_at_ms: first.ts,
        total_ms: Math.max(totalMs, Object.values(breakdown).reduce((s, v) => s + v, 0)),
        stages: group.map(ev => ({ stage: ev.stage, ts: ev.ts, duration_ms: ev.duration_ms ?? null, detail: ev.detail ?? null })),
        component_breakdown: breakdown
      });
    }
  }
  return frames.sort((a, b) => b.started_at_ms - a.started_at_ms);
}

function framesForDeployment(dep, allFrames, limit = FRAMES_PER_DEPLOYMENT) {
  const deviceIds = Array.isArray(dep.device_ids) ? dep.device_ids.map(String) : [];
  const matched = allFrames.filter(f =>
    f.deployment_id === dep.id ||
    (deviceIds.length && deviceIds.includes(String(f.device_id)))
  );
  return matched.slice(0, limit);
}

// ── Mock data (used when backdoor/redis are unavailable) ───────

const MOCK_CATALOGS = ['person_detection', 'ppe_violation', 'fire_detection', 'plate_detected', 'violence_detected'];

function mockDeployments() {
  return MOCK_CATALOGS.slice(0, 3).map((c, i) => ({
    id: `mock-dep-${i}`,
    org_id: ORG_ID,
    catalog_id: `catalog-${c}`,
    workflow_name: c.replace(/_/g, ' ').toUpperCase(),
    deployment_mode: 'one_per_device',
    device_ids: [`dev-mock-${i}`],
    status: 'active',
    active_listeners: 1,
    deployed_at: new Date(nowMs() - (i + 1) * 60 * 60 * 1000).toISOString()
  }));
}

function seedMockData() {
  if (mockSeeded) return;
  mockSeeded = true;
  const types = ['face', 'ppe', 'fire'];
  for (let d = 0; d < 3; d++) {
    const deviceId = `dev-mock-${d}`;
    const depId = `mock-dep-${d}`;
    for (let i = 0; i < 15; i++) {
      const base = nowMs() - (15 - i) * 60 * 1000 - Math.floor(Math.random() * 30000);
      const infMs = Math.floor(Math.random() * 500) + 60;
      const gateMs = Math.floor(Math.random() * 40) + 5;
      const queueMs = Math.floor(Math.random() * 120) + 15;
      const wfMs = Math.floor(Math.random() * 900) + 150;
      const evs = [
        { stage: 'frame_received', event_type: types[d], ts: base },
        { stage: 'inference_done', event_type: types[d], duration_ms: infMs, detected_count: Math.floor(Math.random() * 3), ts: base + 20 + infMs },
        { stage: 'gate_decision', event_type: types[d], duration_ms: gateMs, decision: Math.random() > 0.4 ? 'queued' : 'throttled', ts: base + 30 + infMs + gateMs },
        { stage: 'workflow_queued', event_type: types[d], duration_ms: queueMs, deployment_id: depId, ts: base + 40 + infMs + gateMs + queueMs },
        { stage: 'workflow_done', event_type: types[d], duration_ms: wfMs, deployment_id: depId, ts: base + 50 + infMs + gateMs + queueMs + wfMs }
      ];
      for (const ev of evs) eventStore.push({ org_id: ORG_ID, device_id: deviceId, ...ev });
    }
  }
  clampEvents();
}

// ── Ingestion ──────────────────────────────────────────────────

function ingestEvents(events) {
  if (!Array.isArray(events)) return 0;
  let count = 0;
  for (const ev of events) {
    if (ev && ev.stage) {
      eventStore.push({ ts: nowMs(), ...ev });
      count++;
    }
  }
  clampEvents();
  return count;
}

if (REDIS_URL && !REDIS_URL.startsWith('redis://none')) {
  const redis = createClient({ url: REDIS_URL });
  redis.on('error', () => {});
  redis.connect().then(() => {
    const handler = (message) => {
      try {
        const parsed = JSON.parse(message);
        ingestEvents(Array.isArray(parsed) ? parsed : [parsed]);
      } catch (_) {}
    };
    redis.subscribe('godezk:pipeline:timing', handler);
    redis.subscribe('godezk:execution:new', handler);
  }).catch(() => {});
}

// ── Routes ─────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({
  ok: true,
  events_in_memory: eventStore.length,
  backdoor: BACKDOOR_BASE_URL ? 'configured' : 'disabled'
}));

app.get('/api/components', (_req, res) => res.json({ success: true, components: COMPONENTS }));

// Running deployments, each with its last N frames + per-frame component breakdown
app.get('/api/deployments', async (req, res) => {
  const orgId = req.query.org_id || ORG_ID;
  const limit = Math.min(parseInt(req.query.frames || FRAMES_PER_DEPLOYMENT, 10) || FRAMES_PER_DEPLOYMENT, 100);
  const deployments = await fetchRunningDeployments(orgId);

  const from = parseTime(req.query.date_from);
  const to = parseTime(req.query.date_to);
  let events = eventStore;
  if (from || to) {
    events = eventStore.filter(e => (!from || e.ts >= from) && (!to || e.ts <= to));
  }
  const allFrames = buildFrames(events);

  const rows = deployments.map(dep => {
    const frames = framesForDeployment(dep, allFrames, limit);
    const totals = {};
    for (const f of frames) {
      for (const [c, ms] of Object.entries(f.component_breakdown)) totals[c] = (totals[c] || 0) + ms;
    }
    return {
      id: dep.id,
      workflow_name: dep.workflow_name || dep.catalog_name || dep.catalog_id,
      status: dep.status,
      deployment_mode: dep.deployment_mode,
      device_ids: dep.device_ids || [],
      active_listeners: dep.active_listeners || 0,
      deployed_at: dep.deployed_at,
      frame_count: frames.length,
      component_totals: totals,
      frames
    };
  });

  res.json({ success: true, deployments: rows, total_count: rows.length });
});

// Last N frames for one deployment
app.get('/api/deployments/:id/frames', async (req, res) => {
  const orgId = req.query.org_id || ORG_ID;
  const limit = Math.min(parseInt(req.query.limit || FRAMES_PER_DEPLOYMENT, 10) || FRAMES_PER_DEPLOYMENT, 100);
  const deployments = await fetchRunningDeployments(orgId);
  const dep = deployments.find(d => String(d.id) === String(req.params.id));
  if (!dep) return res.status(404).json({ success: false, error: 'Deployment not found or not running' });
  const frames = framesForDeployment(dep, buildFrames(eventStore), limit);
  res.json({ success: true, deployment_id: dep.id, workflow_name: dep.workflow_name, frames });
});

app.post('/api/ingest/pipeline', (req, res) => {
  const count = ingestEvents(Array.isArray(req.body) ? req.body : [req.body]);
  res.json({ success: true, ingested: count, total_in_memory: eventStore.length });
});

// Analytics across the last frames of running deployments within a time window
app.get('/api/analytics', async (req, res) => {
  let to = parseTime(req.query.to);
  let from = parseTime(req.query.from);
  const windowMs = parseTime(req.query.time_window);
  if (to == null) to = nowMs();
  if (from == null && windowMs != null) from = to - windowMs;
  if (from == null) from = to - 60 * 60 * 1000;

  const orgId = req.query.org_id || ORG_ID;
  const deployments = await fetchRunningDeployments(orgId);

  const allFrames = buildFrames(eventStore.filter(e => e.ts >= from && e.ts <= to));
  let frames = [];
  for (const dep of deployments) frames = frames.concat(framesForDeployment(dep, allFrames, FRAMES_PER_DEPLOYMENT));

  const summary = {
    total: frames.length,
    completed: frames.filter(f => f.stages.some(s => s.stage === 'workflow_done')).length,
    failed: 0,
    running: deployments.length,
    avg_duration_ms: 0,
    total_duration_ms: 0
  };
  const componentStats = {};
  for (const f of frames) {
    summary.total_duration_ms += f.total_ms || 0;
    for (const [comp, ms] of Object.entries(f.component_breakdown)) {
      if (!componentStats[comp]) componentStats[comp] = { total_ms: 0, count: 0, min_ms: Infinity, max_ms: 0, avg_ms: 0 };
      const s = componentStats[comp];
      s.total_ms += ms;
      s.count += 1;
      s.min_ms = Math.min(s.min_ms, ms);
      s.max_ms = Math.max(s.max_ms, ms);
    }
  }
  if (frames.length) summary.avg_duration_ms = Math.round(summary.total_duration_ms / frames.length);
  const componentTotals = Object.entries(componentStats).map(([component, s]) => ({
    component,
    total_ms: Math.round(s.total_ms),
    avg_ms: s.count ? Math.round(s.total_ms / s.count) : 0,
    min_ms: Number.isFinite(s.min_ms) ? Math.round(s.min_ms) : 0,
    max_ms: Math.round(s.max_ms),
    count: s.count
  })).sort((a, b) => b.total_ms - a.total_ms);

  res.json({
    success: true,
    window: { from: new Date(from).toISOString(), to: new Date(to).toISOString(), duration_ms: to - from },
    executions_summary: summary,
    component_totals: componentTotals,
    deployments: deployments.map(d => ({ id: d.id, workflow_name: d.workflow_name, status: d.status }))
  });
});

const distPath = path.join(__dirname, 'dist');
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api')) return next();
    res.sendFile(path.join(distPath, 'index.html'));
  });
}

app.listen(PORT, () => console.log(`telemetry backend on :${PORT}`));
