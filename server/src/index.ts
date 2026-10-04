import express from 'express';
import { appointments, assignTask, blocks, containerDetail, containers, createContainer, createGateAppointment, createYardTask, dashboard, deleteContainer, deleteGateAppointment, deleteYardTask, editGateAppointment, equipment, gateConflicts, operationalAlerts, prioritizeTasks, snapshotModules, tasks, updateContainer, updateYardTask, voyages, yardHeatmap } from './domain.js';
import { opsBus, StreamHub } from './events.js';
import { startTelemetry } from './telemetry.js';

const app = express(); app.use(express.json());

// 实时事件流：统一事件格式 + 游标回放 + 按模块订阅
const hub = new StreamHub(opsBus, snapshotModules);
hub.start();

// 派生视图扇出：实体变更合并触发一次 dashboard/闸口冲突/告警刷新，避免级联写放大
const derivedDirty = new Set<'dashboard' | 'conflicts' | 'alerts'>();
let derivedTimer: ReturnType<typeof setTimeout> | undefined;
opsBus.subscribe(event => {
  if (event.module === 'dashboard' || event.module === 'alerts' || event.type === 'gate.conflicts') return;
  if (event.module !== 'equipment') derivedDirty.add('dashboard');
  derivedDirty.add('alerts');
  if (event.module === 'gate' || event.module === 'containers') derivedDirty.add('conflicts');
  derivedTimer ??= setTimeout(() => {
    derivedTimer = undefined;
    if (derivedDirty.delete('dashboard')) opsBus.publish('dashboard', 'dashboard.updated', dashboard());
    if (derivedDirty.delete('conflicts')) opsBus.publish('gate', 'gate.conflicts', gateConflicts());
    if (derivedDirty.delete('alerts')) opsBus.publish('alerts', 'alerts.updated', operationalAlerts());
  }, 50);
});

startTelemetry();

app.get('/api/events', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
  const connectionId = hub.attach(res, {
    modules: typeof req.query.modules === 'string' ? req.query.modules : undefined,
    lastEventId: typeof req.query.lastEventId === 'string' ? req.query.lastEventId : req.header('Last-Event-ID'),
  });
  req.on('close', () => hub.detach(connectionId));
});
app.get('/api/dashboard', (_req, res) => res.json(dashboard()));
app.get('/api/voyages', (_req, res) => res.json(voyages));
app.get('/api/containers', (req, res) => res.json(req.query.status ? containers.filter(c => c.status === req.query.status) : containers));
app.post('/api/containers', (req, res) => { try { res.status(201).json(createContainer(req.body)); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '创建箱务记录失败' }); } });
app.patch('/api/containers/:id', (req, res) => { try { const result = updateContainer(req.params.id, req.body); if (!result) return res.status(404).json({ message: '箱号不存在' }); res.json(result); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '更新箱务记录失败' }); } });
app.delete('/api/containers/:id', (req, res) => { const result = deleteContainer(req.params.id); if (!result) return res.status(404).json({ message: '箱号不存在' }); res.json({ deleted: true, id: result.id }); });
app.get('/api/containers/:id', (req, res) => { const result = containerDetail(req.params.id); if (!result) return res.status(404).json({ message: '箱号不存在' }); res.json(result); });
app.get('/api/yard/heatmap', (_req, res) => res.json(yardHeatmap()));
app.get('/api/yard/blocks', (_req, res) => res.json(blocks));
app.get('/api/gate/appointments', (_req, res) => res.json(appointments));
app.get('/api/gate/conflicts', (_req, res) => res.json(gateConflicts()));
app.post('/api/gate/appointments', (req, res) => { try { res.status(201).json(createGateAppointment(req.body)); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '预约失败' }); } });
app.patch('/api/gate/appointments/:id', (req, res) => { try { const result = editGateAppointment(req.params.id, req.body); if (!result) return res.status(404).json({ message: '预约不存在' }); res.json(result); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '更新预约失败' }); } });
app.delete('/api/gate/appointments/:id', (req, res) => { const result = deleteGateAppointment(req.params.id); if (!result) return res.status(404).json({ message: '预约不存在' }); res.json({ deleted: true, id: result.id }); });
app.get('/api/equipment', (_req, res) => res.json(equipment));
app.get('/api/tasks', (_req, res) => res.json(prioritizeTasks()));
app.post('/api/tasks', (req, res) => { try { res.status(201).json(createYardTask(req.body)); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '创建作业任务失败' }); } });
app.patch('/api/tasks/:id', (req, res) => { try { const result = updateYardTask(req.params.id, req.body); if (!result) return res.status(404).json({ message: '作业任务不存在' }); res.json(result); } catch (error) { res.status(409).json({ message: error instanceof Error ? error.message : '更新作业任务失败' }); } });
app.delete('/api/tasks/:id', (req, res) => { const result = deleteYardTask(req.params.id); if (!result) return res.status(404).json({ message: '作业任务不存在' }); res.json({ deleted: true, id: result.id }); });
app.patch('/api/tasks/:id/assign', (req, res) => { const result = assignTask(req.params.id, req.body.equipmentId); if (!result) return res.status(404).json({ message: '任务或设备不存在' }); res.json(result); });
app.get('/api/health', (_req, res) => res.json({ ok: true, service: 'port-yard-operations' }));
const port = Number(process.env.PORT || 4107); app.listen(port, () => console.log(`port yard API listening on ${port}`));
