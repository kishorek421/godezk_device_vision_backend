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
const MOCK_EXECUTION_COUNT = 25;

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

const CATALOGS = ['person_detection', 'ppe_violation', 'fire_detection', 'plate_detected', 'violence_detected'];
const STATUSES = ['completed', 'completed', 'completed', 'failed', 'running'];

const eventStore = [];
let mockExecutions = [];

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
    const unit = m[2].toLowerCase();
    const mult = { s: 1000, m: 60 * 1000, h: 60 * 60 * 1000, d: 24 * 60 * 60 * 1000 };
    return Math.round(v * mult[unit]);
  }
  return new Date(str).getTime() || null;
}

function normalizeExecution(row) {
  if (!row) return row;
  if (typeof row.context === 'string') {
    try { row.context = JSON.parse(row.context); } catch (_) { row.context = {}; }
  }
  if (row.started_at_ms == null && row.started_at) row.started_at_ms = new Date(row.started_at).getTime();
  if (row.completed_at_ms == null && row.completed_at) row.completed_at_ms = new Date(row.completed_at).getTime();
  return row;
}

function addBreakdownValue(breakdown, component, ms) {
  if (ms == null) return;
  breakdown[component] = (breakdown[component] || 0) + Number(ms);
}

function buildComponentBreakdown(ex) {
  const breakdown = {};
  const ctx = ex.context || {};
  const runtime = ctx.runtime || {};
  const event = ctx.event || {};

  addBreakdownValue(breakdown, 'inference_service', runtime.ai_time_ms);
  addBreakdownValue(breakdown, 'queuer', runtime.queue_wait_ms);
  addBreakdownValue(breakdown, 'postgresql_task_hydration', runtime.db_time_ms);
  addBreakdownValue(breakdown, 'frame_bus', runtime.bus_time_ms);

  if (runtime.bus && typeof runtime.bus === 'object') {
    for (const k of Object.keys(runtime.bus)) addBreakdownValue(breakdown, 'frame_bus', runtime.bus[k]);
  }

  const nodes = ctx.node_results || {};
  let nodeTotal = 0;
  for (const key of Object.keys(nodes)) {
    nodeTotal += Number(nodes[key]?.duration_ms || 0);
  }
  addBreakdownValue(breakdown, 'graph_nodes', nodeTotal);

  const started = ex.started_at_ms || 0;
  const ended = ex.completed_at_ms || (started + (ex.duration_ms || 0));
  const deviceId = event.device_id || ex.device_id;

  const matching = eventStore.filter(ev =>
    ev && ev.ts != null && ev.ts >= started && ev.ts <= ended &&
    (!deviceId || !ev.device_id || ev.device_id === deviceId)
  );

  for (const ev of matching) {
    const comp = STAGE_TO_COMPONENT[ev.stage] || ev.component;
    addBreakdownValue(breakdown, comp, ev.duration_ms);
  }

  const stageTimings = runtime.stage_timings || {};
  for (const [stage, ms] of Object.entries(stageTimings)) {
    const comp = STAGE_TO_COMPONENT[stage];
    if (comp) addBreakdownValue(breakdown, comp, ms);
    else addBreakdownValue(breakdown, stage, ms);
  }

  if (started && ended && ended >= started) {
    const total = ended - started;
    const known = Object.values(breakdown).reduce((s, v) => s + v, 0);
    const residual = Math.max(0, total - known);
    if (residual > 0) {
      addBreakdownValue(breakdown, 'rtsp_camera', Math.floor(residual / 2));
      addBreakdownValue(breakdown, 'rtsp_handler', residual - Math.floor(residual / 2));
    }
  }

  return breakdown;
}

function enrich(execution) {
  execution.component_breakdown = buildComponentBreakdown(execution);
  return execution;
}

function seedMockData() {
  const baseTs = nowMs() - 7 * 24 * 60 * 60 * 1000;
  mockExecutions = [];
  for (let i = 0; i < MOCK_EXECUTION_COUNT; i++) {
    const catalog = CATALOGS[i % CATALOGS.length];
    const status = STATUSES[i % STATUSES.length];
    const startedAt = baseTs + Math.floor(Math.random() * 7 * 24 * 60 * 60 * 1000);
    const totalMs = Math.floor(Math.random() * 2500) + 350;
    const completedAt = status === 'running' ? null : startedAt + totalMs;
    const deviceId = `dev-mock-${String(i).padStart(4, '0')}`;
    const aiMs = Math.floor(Math.random() * 600) + 50;
    const queueMs = Math.floor(Math.random() * 200) + 20;
    const dbMs = Math.floor(Math.random() * 150) + 10;
    const nodeMs = Math.max(0, totalMs - aiMs - queueMs - dbMs - 100);

    const ex = {
      id: `mock-exec-${i}`,
      org_id: ORG_ID,
      catalog_id: `catalog-${catalog}`,
      catalog_name: catalog.replace(/_/g, ' ').toUpperCase(),
      installation_id: `inst-${i % 5}`,
      status,
      trigger_event: catalog,
      started_at: new Date(startedAt).toISOString(),
      completed_at: completedAt ? new Date(completedAt).toISOString() : null,
      started_at_ms: startedAt,
      completed_at_ms: completedAt,
      duration_ms: completedAt ? totalMs : null,
      seq_no: i + 1,
      device_id: deviceId,
      context: {
        event: { device_id: deviceId, frame_id: `frame-${i}`, timestamp: startedAt },
        runtime: { ai_time_ms: aiMs, queue_wait_ms: queueMs, db_time_ms: dbMs },
        node_results: {
          storage: { duration_ms: Math.floor(nodeMs * 0.3) },
          action: { duration_ms: Math.floor(nodeMs * 0.2) },
          notification: { duration_ms: Math.floor(nodeMs * 0.25) },
          database: { duration_ms: Math.floor(nodeMs * 0.25) }
        }
      }
    };
    mockExecutions.push(ex);

    const events = [
      { stage: 'frame_received', component: 'queuer', duration_ms: queueMs, ts: startedAt + 10 },
      { stage: 'inference_done', component: 'inference_service', duration_ms: aiMs, ts: startedAt + 50 + queueMs },
      { stage: 'gate_decision', component: 'perception_gate', duration_ms: Math.floor(Math.random() * 30) + 5, ts: startedAt + 60 + queueMs + aiMs },
      { stage: 'workflow_queued', component: 'postgresql_workflow_runner_queue', duration_ms: dbMs, ts: startedAt + 80 + queueMs + aiMs },
      { stage: 'workflow_started', component: 'runner_execute_graph', duration_ms: Math.floor(nodeMs * 0.5), ts: startedAt + 100 + queueMs + aiMs + dbMs },
      { stage: 'workflow_done', component: 'postgresql_execution_record', duration_ms: completedAt ? completedAt - startedAt : totalMs, ts: completedAt || startedAt + totalMs }
    ];
    for (const ev of events) eventStore.push({ org_id: ORG_ID, device_id: deviceId, ...ev });
  }
  clampEvents();
}

seedMockData();

function backdoorHeaders(orgId) {
  const headers = { 'x-org-id': orgId || ORG_ID };
  if (BACKDOOR_TOKEN) headers.Authorization = `Bearer ${BACKDOOR_TOKEN}`;
  return headers;
}

async function callBackdoor(method, path, params = {}, orgId) {
  if (!BACKDOOR_BASE_URL) return null;
  try {
    const url = `${BACKDOOR_BASE_URL}${path}`;
    const { data } = await axios({ method, url, params, headers: backdoorHeaders(orgId), timeout: 5000 });
    return data;
  } catch (_) { return null; }
}

function matchesMock(q, ex) {
  if (q.org_id && ex.org_id !== q.org_id) return false;
  if (q.catalog_id && ex.catalog_id !== q.catalog_id) return false;
  if (q.installation_id && ex.installation_id !== q.installation_id) return false;
  if (q.status && ex.status !== q.status) return false;
  if (q.trigger_event && ex.trigger_event !== q.trigger_event) return false;
  if (q.q) {
    const hay = `${ex.id} ${ex.catalog_name} ${ex.trigger_event} ${ex.status}`.toLowerCase();
    if (!hay.includes(String(q.q).toLowerCase())) return false;
  }
  const from = parseTime(q.date_from);
  const to = parseTime(q.date_to);
  if (from && ex.started_at_ms < from) return false;
  if (to && ex.started_at_ms > to) return false;
  return true;
}

function sortMock(executions, sort) {
  if (!sort) return executions;
  const [field, dir] = String(sort).split(':');
  const d = dir === 'asc' ? 1 : -1;
  return [...executions].sort((a, b) => {
    const av = a[field] ?? 0;
    const bv = b[field] ?? 0;
    return (av < bv ? -1 : av > bv ? 1 : 0) * d;
  });
}

async function fetchExecutions(query = {}) {
  const backdoorParams = {
    org_id: query.org_id || ORG_ID,
    date_from: query.date_from,
    date_to: query.date_to,
    page: query.page || 1,
    page_size: query.page_size || 25,
    status: query.status,
    catalog_id: query.catalog_id,
    installation_id: query.installation_id,
    q: query.q
  };
  const data = await callBackdoor('get', '/api/workflows/executions', backdoorParams, query.org_id);
  if (data && data.success && Array.isArray(data.executions)) {
    return {
      success: true,
      executions: data.executions.map(normalizeExecution).map(enrich),
      total_count: data.total_count || data.executions.length,
      page: parseInt(data.page || backdoorParams.page, 10),
      page_size: parseInt(data.page_size || backdoorParams.page_size, 10)
    };
  }

  let filtered = mockExecutions.filter(e => matchesMock(query, e));
  if (query.sort) filtered = sortMock(filtered, query.sort);
  const page = Math.max(1, parseInt(query.page || 1, 10));
  const pageSize = Math.max(1, parseInt(query.page_size || 25, 10));
  const start = (page - 1) * pageSize;
  const pageItems = filtered.slice(start, start + pageSize);
  return {
    success: true,
    executions: pageItems.map(enrich),
    total_count: filtered.length,
    page,
    page_size: pageSize
  };
}

async function fetchExecution(id, orgId) {
  const data = await callBackdoor('get', `/api/workflows/executions/${id}`, {}, orgId);
  if (data && data.success && data.execution) return { success: true, execution: enrich(normalizeExecution(data.execution)) };
  if (data && data.execution) return { success: true, execution: enrich(normalizeExecution(data.execution)) };
  const found = mockExecutions.find(e => e.id === id);
  if (found) return { success: true, execution: enrich(normalizeExecution({ ...found })) };
  return { success: false, message: 'execution not found' };
}

function ingestEvents(events) {
  if (!Array.isArray(events)) return 0;
  for (const ev of events) {
    if (ev && ev.ts != null) eventStore.push(ev);
  }
  clampEvents();
  return events.length;
}

if (REDIS_URL && !REDIS_URL.startsWith('redis://none')) {
  const redis = createClient({ url: REDIS_URL });
  redis.on('error', () => {});
  redis.connect().then(() => {
    redis.subscribe('godezk:pipeline:timing', (message) => {
      try {
        const parsed = JSON.parse(message);
        if (Array.isArray(parsed)) ingestEvents(parsed);
        else if (parsed) ingestEvents([parsed]);
      } catch (_) {}
    });
    redis.subscribe('godezk:execution:new', (message) => {
      try {
        const parsed = JSON.parse(message);
        if (Array.isArray(parsed)) ingestEvents(parsed);
        else if (parsed) ingestEvents([parsed]);
      } catch (_) {}
    });
  }).catch(() => {});
}

app.get('/health', (_req, res) => res.json({
  ok: true,
  events_in_memory: eventStore.length,
  mock_executions: mockExecutions.length,
  backdoor: BACKDOOR_BASE_URL ? 'configured' : 'disabled'
}));

app.get('/api/components', (_req, res) => res.json({ success: true, components: COMPONENTS }));

app.get('/api/executions', async (req, res) => {
  const result = await fetchExecutions(req.query);
  res.status(result.success ? 200 : 503).json(result);
});

app.get('/api/executions/:id', async (req, res) => {
  const result = await fetchExecution(req.params.id, req.query.org_id);
  res.status(result.success ? 200 : 404).json(result);
});

app.post('/api/ingest/pipeline', (req, res) => {
  const count = ingestEvents(req.body);
  res.json({ success: true, ingested: count, total_in_memory: eventStore.length });
});

app.get('/api/analytics', async (req, res) => {
  let from = parseTime(req.query.from);
  let to = parseTime(req.query.to);
  const window = parseTime(req.query.time_window);
  if (to == null) to = nowMs();
  if (from == null && window != null) from = to - window;
  if (from == null) from = to - 60 * 60 * 1000;

  const result = await fetchExecutions({
    org_id: req.query.org_id,
    date_from: from,
    date_to: to,
    page: 1,
    page_size: 10000
  });

  const executions = result.success ? result.executions : [];
  const summary = {
    total: executions.length,
    completed: 0,
    failed: 0,
    running: 0,
    avg_duration_ms: 0,
    total_duration_ms: 0
  };
  const componentStats = {};

  for (const ex of executions) {
    summary[ex.status]++;
    const dur = ex.duration_ms || (ex.completed_at_ms && ex.started_at_ms ? ex.completed_at_ms - ex.started_at_ms : 0);
    if (dur > 0) summary.total_duration_ms += dur;
    for (const [comp, ms] of Object.entries(ex.component_breakdown || {})) {
      if (!componentStats[comp]) componentStats[comp] = { total_ms: 0, count: 0, min_ms: Infinity, max_ms: 0, avg_ms: 0 };
      componentStats[comp].total_ms += ms;
      componentStats[comp].count += 1;
      componentStats[comp].min_ms = Math.min(componentStats[comp].min_ms, ms);
      componentStats[comp].max_ms = Math.max(componentStats[comp].max_ms, ms);
    }
  }

  if (executions.length) summary.avg_duration_ms = Math.round(summary.total_duration_ms / executions.length);
  for (const c of Object.keys(componentStats)) {
    const s = componentStats[c];
    s.avg_ms = s.count ? Math.round(s.total_ms / s.count) : 0;
    if (!Number.isFinite(s.min_ms)) s.min_ms = 0;
  }

  res.json({
    success: true,
    from,
    to,
    summary,
    component_stats: componentStats,
    executions_count: executions.length
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
