require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { createClient } = require('redis');
const path = require('path');

const PORT = process.env.PORT || 3010;
const BACKDOOR_BASE_URL = (process.env.BACKDOOR_BASE_URL || 'http://localhost:8090').replace(/\/$/, '');
const BACKDOOR_TOKEN = process.env.BACKDOOR_TOKEN || null;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const ORG_ID = process.env.ORG_ID || 'default';
const ENABLE_MOCK = process.env.ENABLE_MOCK !== 'false';
const MAX_EVENTS = parseInt(process.env.MAX_EVENTS || '100000', 10);
const MOCK_EXECUTION_COUNT = parseInt(process.env.MOCK_EXECUTION_COUNT || '25', 10);

const CATALOGS = ['person_detection', 'ppe_violation', 'fire_detection', 'plate_detected', 'violence_detected'];
const STATUSES = ['completed', 'completed', 'completed', 'failed', 'running'];

const COMPONENT_NAMES = [
  'rtsp_camera',
  'rtsp_handler',
  'function_adapter',
  'device_connection_manager',
  'frame_weir',
  'frame_bus',
  'queuer',
  'pre_screener',
  'perception_gate',
  'inference_service',
  'semantic_event',
  'redis_cooldown',
  'postgresql_workflow_runner_queue',
  'queue_handler',
  'redis_executor_task_channel',
  'executor_worker_pool',
  'postgresql_task_hydration',
  'runner_execute_graph',
  'graph_nodes',
  'postgresql_execution_record',
  'redis_telemetry_events',
];

const STAGE_TO_COMPONENT = {
  frame_received: 'queuer',
  inference_start: 'inference_service',
  inference_done: 'inference_service',
  gate_decision: 'perception_gate',
  workflow_queued: 'postgresql_workflow_runner_queue',
  workflow_started: 'runner_execute_graph',
  workflow_done: 'runner_execute_graph',
};

const eventStore = [];
let mockExecutions = [];

function nowMs() {
  return Date.now();
}

function parseTime(value) {
  if (value == null) return null;
  const str = String(value).trim();
  const ms = Number(str);
  if (!Number.isNaN(ms)) return ms;
  const match = str.match(/^(\d+(?:\.\d+)?)\s*([smhd])$/i);
  if (!match) return new Date(str).getTime() || null;
  const n = parseFloat(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers = { s: 1000, m: 60 * 1000, h: 60 * 60 * 1000, d: 24 * 60 * 60 * 1000 };
  return Math.round(n * multipliers[unit]);
}

function clampEvents() {
  if (eventStore.length > MAX_EVENTS) {
    eventStore.splice(0, eventStore.length - MAX_EVENTS);
  }
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

function buildComponentBreakdown(execution) {
  const breakdown = {};
  const ctx = execution.context || {};
  const runtime = ctx.runtime || {};
  const event = ctx.event || {};

  if (runtime.ai_time_ms != null) breakdown.inference_service = (breakdown.inference_service || 0) + runtime.ai_time_ms;
  if (runtime.queue_wait_ms != null) breakdown.queuer = (breakdown.queuer || 0) + runtime.queue_wait_ms;
  if (runtime.db_time_ms != null) breakdown.postgresql_task_hydration = (breakdown.postgresql_task_hydration || 0) + runtime.db_time_ms;

  const nodes = ctx.node_results || {};
  let nodeTotal = 0;
  for (const key of Object.keys(nodes)) {
    nodeTotal += Number(nodes[key]?.duration_ms || 0);
  }
  if (nodeTotal) breakdown.graph_nodes = (breakdown.graph_nodes || 0) + nodeTotal;

  const started = execution.started_at_ms || new Date(execution.started_at).getTime() || 0;
  const ended = execution.completed_at_ms || new Date(execution.completed_at).getTime() || started + (execution.duration_ms || 0);
  const deviceId = event.device_id || event.camera_id || execution.device_id;

  const matching = eventStore.filter(ev => {
    if (ev == null || ev.ts == null) return false;
    if (ev.ts < started || ev.ts > ended) return false;
    if (deviceId && ev.device_id && ev.device_id !== deviceId) return false;
    return true;
  });

  for (const ev of matching) {
    const comp = STAGE_TO_COMPONENT[ev.stage] || ev.component || ev.stage;
    if (comp && ev.duration_ms != null) {
      breakdown[comp] = (breakdown[comp] || 0) + Number(ev.duration_ms);
    }
  }

  return breakdown;
}

function humanize(ms) {
  if (ms == null || Number.isNaN(ms)) return '-';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60 * 1000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / (60 * 1000)).toFixed(2)}m`;
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
    const execId = `mock-exec-${i}`;
    const seqNo = i + 1;
    const aiMs = Math.floor(Math.random() * 600) + 50;
    const queueMs = Math.floor(Math.random() * 200) + 20;
    const dbMs = Math.floor(Math.random() * 150) + 10;
    const nodeMs = Math.max(0, totalMs - aiMs - queueMs - dbMs - 100);

    const execution = {
      id: execId,
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
      seq_no: seqNo,
      context: {
        event: { device_id: deviceId, frame_id: `frame-${i}`, timestamp: startedAt },
        runtime: { ai_time_ms: aiMs, queue_wait_ms: queueMs, db_time_ms: dbMs },
        node_results: {
          storage: { duration_ms: Math.floor(nodeMs * 0.3) },
          notification: { duration_ms: Math.floor(nodeMs * 0.2) },
          database: { duration_ms: Math.floor(nodeMs * 0.3) },
          action: { duration_ms: Math.max(0, nodeMs - Math.floor(nodeMs * 0.8)) },
        },
      },
      human_id: `${catalog.substring(0, 2).toUpperCase()}-${String(seqNo).padStart(6, '0')}-${execId.slice(-7)}`,
      error_message: status === 'failed' ? 'Simulated failure' : null,
    };

    mockExecutions.push(execution);

    const components = [
      { stage: 'frame_received', component: 'queuer', duration_ms: queueMs, ts: startedAt + 10 },
      { stage: 'inference_done', component: 'inference_service', duration_ms: aiMs, ts: startedAt + 50 + queueMs },
      { stage: 'gate_decision', component: 'perception_gate', duration_ms: Math.floor(Math.random() * 30) + 5, ts: startedAt + 60 + queueMs + aiMs },
      { stage: 'workflow_queued', component: 'postgresql_workflow_runner_queue', duration_ms: dbMs, ts: startedAt + 80 + queueMs + aiMs },
      { stage: 'workflow_started', component: 'runner_execute_graph', duration_ms: Math.floor(nodeMs * 0.5), ts: startedAt + 100 + queueMs + aiMs + dbMs },
      { stage: 'workflow_done', component: 'postgresql_execution_record', duration_ms: completedAt ? completedAt - startedAt : totalMs, ts: completedAt || startedAt + totalMs },
    ];

    for (const ev of components) {
      eventStore.push({ org_id: ORG_ID, device_id: deviceId, event_type: catalog, ...ev });
    }
  }
  clampEvents();
}

function backdoorHeaders(orgId) {
  const headers = { 'x-org-id': orgId || ORG_ID };
  if (BACKDOOR_TOKEN) headers.Authorization = `Bearer ${BACKDOOR_TOKEN}`;
  return headers;
}

async function fetchBackdoorExecutions(query) {
  if (BACKDOOR_BASE_URL === 'none') return null;
  const url = `${BACKDOOR_BASE_URL}/api/workflows/executions`;
  try {
    const { data } = await axios.get(url, { params: query, headers: backdoorHeaders(query.org_id), timeout: 3000 });
    return data.success ? data : null;
  } catch (err) {
    return null;
  }
}

async function fetchBackdoorExecution(id, orgId) {
  if (BACKDOOR_BASE_URL === 'none') return null;
  const url = `${BACKDOOR_BASE_URL}/api/workflows/executions/${encodeURIComponent(id)}`;
  try {
    const { data } = await axios.get(url, { params: { org_id: orgId || ORG_ID }, headers: backdoorHeaders(orgId), timeout: 3000 });
    return data.success ? data : null;
  } catch (err) {
    return null;
  }
}

function filterMockExecutions(query) {
  let rows = [...mockExecutions];
  if (query.q) {
    const q = String(query.q).toLowerCase();
    rows = rows.filter(r => (r.human_id || '').toLowerCase().includes(q) || (r.catalog_name || '').toLowerCase().includes(q));
  }
  if (query.status) {
    const statuses = String(query.status).split(',').filter(Boolean);
    rows = rows.filter(r => statuses.includes(r.status));
  }
  if (query.date_from) {
    const from = new Date(query.date_from).getTime();
    rows = rows.filter(r => r.started_at_ms >= from);
  }
  if (query.date_to) {
    const to = new Date(query.date_to).getTime();
    rows = rows.filter(r => (r.completed_at_ms || r.started_at_ms) <= to);
  }
  if (query.catalog_id) rows = rows.filter(r => r.catalog_id === query.catalog_id);
  if (query.installation_id) rows = rows.filter(r => r.installation_id === query.installation_id);
  if (query.device_id) rows = rows.filter(r => (r.context?.event?.device_id || r.device_id) === query.device_id);

  const sort = String(query.sort || 'started_at_desc').toLowerCase();
  rows.sort((a, b) => sort === 'started_at_asc' ? a.started_at_ms - b.started_at_ms : b.started_at_ms - a.started_at_ms);

  const page = Math.max(parseInt(query.page, 10) || 1, 1);
  const pageSize = Math.min(Math.max(parseInt(query.page_size, 10) || 25, 1), 500);
  const offset = (page - 1) * pageSize;
  const paginated = rows.slice(offset, offset + pageSize);

  return {
    success: true,
    executions: paginated.map(r => ({ ...r, component_breakdown: buildComponentBreakdown(r) })),
    total_count: rows.length,
    page,
    page_size: pageSize,
  };
}

function aggregateComponentTotals(fromMs, toMs) {
  const filtered = eventStore.filter(e => e.ts >= fromMs && e.ts <= toMs);
  const byComp = {};
  for (const ev of filtered) {
    const comp = STAGE_TO_COMPONENT[ev.stage] || ev.component || ev.stage;
    if (!comp || ev.duration_ms == null) continue;
    const dur = Number(ev.duration_ms);
    if (!byComp[comp]) byComp[comp] = { total_ms: 0, count: 0, min_ms: dur, max_ms: dur };
    byComp[comp].total_ms += dur;
    byComp[comp].count += 1;
    byComp[comp].min_ms = Math.min(byComp[comp].min_ms, dur);
    byComp[comp].max_ms = Math.max(byComp[comp].max_ms, dur);
  }
  const totals = Object.entries(byComp)
    .map(([component, v]) => ({
      component,
      total_ms: Math.round(v.total_ms),
      avg_ms: v.count ? Math.round(v.total_ms / v.count) : 0,
      min_ms: Math.round(v.min_ms),
      max_ms: Math.round(v.max_ms),
      count: v.count,
    }))
    .sort((a, b) => b.total_ms - a.total_ms);
  return totals;
}

function summarizeExecutions(rows) {
  const completed = rows.filter(r => r.status === 'completed');
  const failed = rows.filter(r => r.status === 'failed');
  const running = rows.filter(r => r.status === 'running');
  const durations = rows.map(r => r.duration_ms).filter(v => v != null);
  const totalDuration = durations.reduce((s, v) => s + v, 0);
  return {
    total: rows.length,
    completed: completed.length,
    failed: failed.length,
    running: running.length,
    total_duration_ms: Math.round(totalDuration),
    avg_duration_ms: durations.length ? Math.round(totalDuration / durations.length) : 0,
  };
}

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'telemetry-dashboard', events: eventStore.length, executions: mockExecutions.length });
});

app.get('/api/components', (req, res) => {
  res.json({ components: COMPONENT_NAMES });
});

app.get('/api/executions', async (req, res) => {
  const query = { ...req.query };
  query.org_id = query.org_id || ORG_ID;

  const backdoorData = await fetchBackdoorExecutions(query);
  if (backdoorData) {
    const rows = (backdoorData.executions || []).map(normalizeExecution).map(r => ({
      ...r,
      component_breakdown: buildComponentBreakdown(r),
      component_total_ms: Object.values(buildComponentBreakdown(r)).reduce((s, v) => s + v, 0),
    }));
    return res.json({
      ...backdoorData,
      executions: rows,
    });
  }

  if (ENABLE_MOCK) {
    return res.json(filterMockExecutions(query));
  }

  res.status(503).json({ success: false, error: 'Backdoor unavailable and mock data disabled' });
});

app.get('/api/executions/:id', async (req, res) => {
  const { id } = req.params;
  const orgId = req.query.org_id || ORG_ID;
  const backdoor = await fetchBackdoorExecution(id, orgId);
  if (backdoor && backdoor.execution) {
    const row = normalizeExecution(backdoor.execution);
    row.component_breakdown = buildComponentBreakdown(row);
    return res.json({ ...backdoor, execution: row });
  }

  if (ENABLE_MOCK) {
    const row = mockExecutions.find(e => e.id === id || e.human_id === id);
    if (row) {
      const r = normalizeExecution({ ...row });
      r.component_breakdown = buildComponentBreakdown(r);
      return res.json({ success: true, execution: r, runtime_context: r.context });
    }
  }

  res.status(404).json({ success: false, error: 'Execution not found' });
});

app.get('/api/analytics', async (req, res) => {
  let toMs = req.query.to ? parseTime(req.query.to) : nowMs();
  let fromMs = req.query.from ? parseTime(req.query.from) : null;
  const windowMs = parseTime(req.query.time_window);
  if (windowMs != null) {
    toMs = toMs || nowMs();
    fromMs = toMs - windowMs;
  }
  if (fromMs == null) fromMs = toMs - 60 * 60 * 1000;

  const orgId = req.query.org_id || ORG_ID;

  const backdoorQuery = {
    org_id: orgId,
    date_from: new Date(fromMs).toISOString(),
    date_to: new Date(toMs).toISOString(),
    page: 1,
    page_size: 500,
  };

  let executions = [];
  const backdoorData = await fetchBackdoorExecutions(backdoorQuery);
  if (backdoorData) {
    executions = (backdoorData.executions || []).map(normalizeExecution);
  } else if (ENABLE_MOCK) {
    executions = filterMockExecutions({ ...backdoorQuery, date_from: new Date(fromMs).toISOString(), date_to: new Date(toMs).toISOString() }).executions;
  }

  const execSummary = summarizeExecutions(executions);
  const componentTotals = aggregateComponentTotals(fromMs, toMs);

  const resolution = parseTime(req.query.resolution);
  let timeline = [];
  if (resolution != null && resolution > 0) {
    const buckets = [];
    for (let t = Math.floor(fromMs / resolution) * resolution; t < toMs; t += resolution) {
      buckets.push({ start: t, end: Math.min(t + resolution, toMs), events: {} });
    }
    for (const ev of eventStore.filter(e => e.ts >= fromMs && e.ts <= toMs)) {
      const bucket = buckets.find(b => ev.ts >= b.start && ev.ts < b.end) || buckets[buckets.length - 1];
      if (!bucket) continue;
      const comp = STAGE_TO_COMPONENT[ev.stage] || ev.component || ev.stage;
      if (!comp || ev.duration_ms == null) continue;
      bucket.events[comp] = (bucket.events[comp] || 0) + Number(ev.duration_ms);
    }
    timeline = buckets.map(b => ({
      start: new Date(b.start).toISOString(),
      end: new Date(b.end).toISOString(),
      components: b.events,
    }));
  }

  res.json({
    success: true,
    window: { from: new Date(fromMs).toISOString(), to: new Date(toMs).toISOString(), duration_ms: toMs - fromMs },
    executions_summary: execSummary,
    component_totals: componentTotals,
    timeline,
  });
});

app.post('/api/ingest/pipeline', (req, res) => {
  const events = Array.isArray(req.body) ? req.body : [req.body];
  const stored = [];
  for (const ev of events) {
    if (!ev || !ev.stage) continue;
    const item = {
      ...ev,
      org_id: ev.org_id || ORG_ID,
      ts: ev.ts || nowMs(),
    };
    eventStore.push(item);
    stored.push(item);
  }
  clampEvents();
  res.json({ success: true, stored: stored.length });
});

app.use(express.static(path.join(__dirname, '../godezk_device_vision_frontend/dist')));
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

async function startRedis() {
  if (REDIS_URL === 'none' || !REDIS_URL) return;
  try {
    const sub = createClient({ url: REDIS_URL });
    sub.on('error', () => {});
    await sub.connect();
    await sub.subscribe('godezk:pipeline:timing', (message) => {
      try {
        const ev = JSON.parse(message);
        if (ev && ev.ts) eventStore.push(ev);
        clampEvents();
      } catch (_) {}
    });
    await sub.subscribe('godezk:execution:new', (message) => {
      try {
        const ev = JSON.parse(message);
        if (ev && ev.started_at) {
          eventStore.push({ stage: 'execution_record', component: 'postgresql_execution_record', ...ev, ts: new Date(ev.started_at).getTime() });
          clampEvents();
        }
      } catch (_) {}
    });
  } catch (err) {
    console.error('Redis connect failed:', err.message);
  }
}

if (ENABLE_MOCK) seedMockData();

app.listen(PORT, async () => {
  console.log(`Telemetry dashboard backend listening on http://localhost:${PORT}`);
  await startRedis();
});
