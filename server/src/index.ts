import express from 'express';
import { appointments, assignTask, blocks, containerDetail, containers, createContainer, createGateAppointment, createYardTask, dashboard, deleteContainer, deleteGateAppointment, deleteYardTask, editGateAppointment, equipment, gateConflicts, prioritizeTasks, setDomainEmitter, tasks, updateContainer, updateYardTask, voyages, yardHeatmap } from './domain.js';
import { ALL_MODULES, bus, eventsHandler, sseClientCount, type OpsModule } from './events.js';

// 领域实体 -> 推送模块映射；所有变更统一走 OpsEvent 格式
const ENTITY_MODULE: Record<string, OpsModule> = { voyage: 'voyages', container: 'containers', appointment: 'appointments', task: 'tasks', equipment: 'equipment', block: 'yard' };
setDomainEmitter((entity, action, entityId, data) => {
  const module = ENTITY_MODULE[entity];
  if (module) bus.publish(module, action, entityId, data);
  publishDerived();
});

export interface AlertItem { id: string; severity: 'critical' | 'warning'; module: OpsModule; title: string; detail: string }

function computeAlerts(): AlertItem[] {
  const alerts: AlertItem[] = [];
  for (const container of containers) {
    if (container.reefer && ((container.temperature ?? 0) > 5 || container.customs === 'inspection'))
      alerts.push({ id: `reefer:${container.id}`, severity: 'warning', module: 'containers', title: `冷藏箱 ${container.id} 温控/查验风险`, detail: `温度 ${container.temperature ?? '-'}°C · 海关 ${container.customs}` });
    if (container.hazmatClass && container.customs !== 'released')
      alerts.push({ id: `hazmat:${container.id}`, severity: 'critical', module: 'containers', title: `危险品 ${container.id} 未放行`, detail: `${container.hazmatClass} 类 · 海关 ${container.customs}` });
  }
  for (const task of tasks.filter(item => item.status === 'blocked'))
    alerts.push({ id: `blocked:${task.id}`, severity: 'critical', module: 'tasks', title: `任务 ${task.id} 阻塞`, detail: task.blockingReasons[0] ?? '未知原因' });
  for (const conflict of gateConflicts())
    alerts.push({ id: `gate:${conflict.appointmentId}:${conflict.reason.slice(0, 12)}`, severity: conflict.severity, module: 'appointments', title: `闸口冲突 ${conflict.appointmentId}`, detail: conflict.reason });
  for (const block of blocks.filter(item => item.heat > 85))
    alerts.push({ id: `heat:${block.id}`, severity: 'warning', module: 'yard', title: `区块 ${block.id} 堆存过热`, detail: `热度 ${block.heat} · 利用率 ${Math.round(block.occupied / block.capacity * 100)}%` });
  return alerts;
}

// 派生模块：dashboard 指标、告警集合、堆场热区随任意实体变更重算并整体推送
let derivedTimer: NodeJS.Timeout | undefined;
function publishDerived() {
  clearTimeout(derivedTimer);
  derivedTimer = setTimeout(() => {
    bus.publish('dashboard', 'upsert', 'current', dashboard());
    bus.publish('alerts', 'upsert', 'current', computeAlerts());
    bus.publish('yard', 'upsert', 'heatmap', yardHeatmap());
  }, 25); // 合并一次请求内的级联变更，避免事件风暴
}

// 遥测模拟：让船期翻箱进度、设备电量/队列、区块热度持续产生增量事件
const rand = (min: number, max: number) => Math.floor(min + Math.random() * (max - min + 1));
setInterval(() => {
  for (const voyage of voyages.filter(item => item.status === 'berthed' || item.status === 'loading')) {
    if (voyage.movesDone < voyage.movesPlanned && Math.random() < 0.7) {
      voyage.movesDone = Math.min(voyage.movesPlanned, voyage.movesDone + rand(1, 4));
      bus.publish('voyages', 'upsert', voyage.id, voyage);
    }
  }
  for (const machine of equipment) {
    machine.battery = Math.max(8, Math.min(100, machine.battery + (machine.status === 'working' ? -rand(0, 2) : rand(0, 1))));
    machine.queue = Math.max(0, machine.queue + rand(-1, 1));
    machine.movesPerHour = Math.max(4, machine.movesPerHour + rand(-1, 1));
    bus.publish('equipment', 'upsert', machine.id, machine);
  }
  for (const block of blocks) block.heat = Math.max(20, Math.min(99, block.heat + rand(-3, 3)));
  publishDerived();
}, 8000).unref();

const app = express(); app.use(express.json());
app.get('/api/events', eventsHandler(modules => {
  const snapshot: { module: OpsModule; entityId: string; data: unknown }[] = [];
  if (modules.has('voyages')) snapshot.push({ module: 'voyages', entityId: 'list', data: voyages });
  if (modules.has('containers')) snapshot.push({ module: 'containers', entityId: 'list', data: containers });
  if (modules.has('appointments')) snapshot.push({ module: 'appointments', entityId: 'list', data: appointments });
  if (modules.has('tasks')) snapshot.push({ module: 'tasks', entityId: 'list', data: prioritizeTasks() });
  if (modules.has('equipment')) snapshot.push({ module: 'equipment', entityId: 'list', data: equipment });
  if (modules.has('yard')) snapshot.push({ module: 'yard', entityId: 'heatmap', data: yardHeatmap() });
  if (modules.has('alerts')) snapshot.push({ module: 'alerts', entityId: 'current', data: computeAlerts() });
  if (modules.has('dashboard')) snapshot.push({ module: 'dashboard', entityId: 'current', data: dashboard() });
  return snapshot;
}));
app.get('/api/events/status', (_req, res) => res.json({ clients: sseClientCount(), cursor: bus.currentSeq(), modules: ALL_MODULES }));
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
