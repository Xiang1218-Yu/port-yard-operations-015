import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, StreamHub, formatSse, parseModules, type OpsEvent } from './events.js';

class FakeSink {
  chunks: string[] = [];
  ended = false;
  writableLength = 0;
  destroyed = false;
  write(chunk: string) { this.chunks.push(chunk); return true; }
  end() { this.ended = true; }
  get text() { return this.chunks.join(''); }
  frames() {
    return this.text.split('\n\n').filter(Boolean).map(raw => {
      const lines = raw.split('\n');
      const event = lines.find(line => line.startsWith('event:'))?.slice(7);
      const id = lines.find(line => line.startsWith('id:'))?.slice(4);
      const data = lines.find(line => line.startsWith('data:'))?.slice(6);
      return { event, id: id ? Number(id) : undefined, data: data ? JSON.parse(data) : undefined };
    });
  }
}

test('formatSse emits event, id and data lines; control frames omit id', () => {
  assert.equal(formatSse('ops', { a: 1 }, 7), 'event: ops\nid: 7\ndata: {"a":1}\n\n');
  assert.equal(formatSse('heartbeat', { seq: 7 }), 'event: heartbeat\ndata: {"seq":7}\n\n');
});

test('parseModules falls back to all modules and drops unknown names', () => {
  assert.equal(parseModules(undefined).size, 8);
  assert.deepEqual([...parseModules('containers,bogus,tasks')], ['containers', 'tasks']);
});

test('event bus assigns monotonic cursors and evicts old events beyond capacity', () => {
  const bus = new EventBus(3, () => new Date('2026-10-04T00:00:00Z'));
  for (let index = 0; index < 5; index++) bus.publish('containers', 'container.updated', { index }, `C-${index}`);
  assert.equal(bus.cursor, 5);
  assert.equal(bus.oldest, 3);
  assert.equal(bus.covers(2), true);
  assert.equal(bus.covers(1), false);
  assert.deepEqual(bus.replay(3, new Set(['containers'])).map(event => event.id), [4, 5]);
  assert.deepEqual(bus.replay(3, new Set(['tasks'])), []);
});

test('a throwing subscriber is isolated and does not break other subscribers', () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe(() => { throw new Error('boom'); });
  bus.subscribe(event => received.push(event.id));
  assert.doesNotThrow(() => bus.publish('tasks', 'task.updated', {}));
  assert.deepEqual(received, [1]);
});

test('stream hub sends snapshot first, then only subscribed live deltas in order', () => {
  const bus = new EventBus();
  bus.publish('containers', 'container.created', { id: 'C-1' }, 'C-1');
  const hub = new StreamHub(bus, modules => ({ requested: [...modules] }));
  const sink = new FakeSink();
  hub.attach(sink, { modules: 'containers' });
  bus.publish('containers', 'container.updated', { id: 'C-1' }, 'C-1');
  bus.publish('tasks', 'task.updated', { id: 'T-1' }, 'T-1');
  const frames = sink.frames();
  assert.equal(frames[0].event, 'snapshot');
  assert.equal(frames[0].id, 1);
  assert.deepEqual(frames[0].data.modules.requested, ['containers']);
  assert.deepEqual(frames.slice(1).map(frame => [frame.event, frame.id]), [['ops', 2]]);
});

test('events published while the snapshot is being built are replayed right after it', () => {
  const bus = new EventBus();
  const hub = new StreamHub(bus, () => { bus.publish('gate', 'appointment.created', { id: 'A-1' }, 'A-1'); return { appointments: [] }; });
  const sink = new FakeSink();
  hub.attach(sink, {});
  const frames = sink.frames();
  assert.equal(frames[0].event, 'snapshot');
  assert.equal(frames[0].id, 0);
  assert.deepEqual(frames.slice(1).map(frame => [frame.event, frame.id]), [['ops', 1]]);
});

test('reconnect with last cursor replays only missed events without a new snapshot', () => {
  const bus = new EventBus();
  bus.publish('tasks', 'task.created', { id: 'T-1' }, 'T-1');
  const hub = new StreamHub(bus, () => ({}));
  hub.attach(new FakeSink(), {});
  bus.publish('tasks', 'task.updated', { id: 'T-1' }, 'T-1');
  bus.publish('tasks', 'task.updated', { id: 'T-1' }, 'T-1');
  const sink = new FakeSink();
  hub.attach(sink, { lastEventId: '1' });
  assert.deepEqual(sink.frames().map(frame => [frame.event, frame.id]), [['ops', 2], ['ops', 3]]);
});

test('a cursor evicted from the buffer triggers resync instead of silent gaps', () => {
  const bus = new EventBus(2);
  const hub = new StreamHub(bus, () => ({}));
  bus.publish('tasks', 'task.updated', {}, 'T');
  bus.publish('tasks', 'task.updated', {}, 'T');
  bus.publish('tasks', 'task.updated', {}, 'T');
  bus.publish('tasks', 'task.updated', {}, 'T'); // 缓冲区只剩 [3,4]，游标 1 之后的事件 2 已丢失
  const sink = new FakeSink();
  hub.attach(sink, { lastEventId: '1' });
  const frames = sink.frames();
  assert.equal(frames.length, 1);
  assert.equal(frames[0].event, 'resync');
  assert.equal(sink.ended, true);
  assert.equal(hub.size, 0);
});

test('a broken connection is detached without affecting other connections', () => {
  const bus = new EventBus();
  const hub = new StreamHub(bus, () => ({}));
  const broken = new FakeSink();
  broken.write = () => { throw new Error('EPIPE'); };
  const healthy = new FakeSink();
  hub.attach(broken, {});
  hub.attach(healthy, {});
  bus.publish('equipment', 'equipment.updated', { id: 'QC-07' }, 'QC-07');
  assert.equal(hub.size, 1);
  assert.ok(healthy.text.includes('equipment.updated'));
});

test('heartbeat refreshes write activity; sweep removes stale, backlogged and expired connections', () => {
  let now = 1_000_000;
  const bus = new EventBus();
  const hub = new StreamHub(bus, () => ({}), { now: () => now, staleMs: 90_000, maxBufferedBytes: 1000, maxAgeMs: 600_000 });
  const stale = new FakeSink();
  hub.attach(stale, {});
  now += 30_000;
  hub.heartbeat();
  assert.ok(stale.text.includes('event: heartbeat'));
  now += 100_000; // 超过 staleMs，期间没有任何成功写入
  const backlogged = new FakeSink();
  hub.attach(backlogged, {});
  backlogged.writableLength = 5000;
  const fresh = new FakeSink();
  hub.attach(fresh, {});
  hub.sweep();
  assert.equal(stale.ended, true);
  assert.equal(backlogged.ended, true);
  assert.equal(fresh.ended, false);
  assert.equal(hub.size, 1);
  now += 600_001; // 超过 maxAgeMs，触发再均衡
  hub.heartbeat(); // 心跳照常刷新写入时间，确保是超龄而不是僵死触发清理
  hub.sweep();
  assert.equal(fresh.ended, true);
  assert.ok(fresh.text.includes('event: rebalance'));
  assert.equal(hub.size, 0);
});

test('domain mutations publish unified events including cascades and monotonic cursors', async () => {
  const { createContainer, deleteContainer, createGateAppointment, editGateAppointment, createYardTask, assignTask } = await import('./domain.js');
  const { opsBus } = await import('./events.js');
  const seen: OpsEvent[] = [];
  const unsubscribe = opsBus.subscribe(event => seen.push(event));
  const container = createContainer({ id: 'TEST-EVT-0001', voyageId: 'VOY-AX7', size: '20', isoType: '22G1', direction: 'import', status: 'in-yard', weightT: 10, reefer: false, customs: 'released', priority: 'normal', yardSlot: 'C03-R12-T1' });
  const appointment = createGateAppointment({ containerId: container.id, truckNo: 'EVT-01', driver: '测试', type: 'dropoff', windowStart: new Date(Date.now() + 3_600_000).toISOString(), windowEnd: new Date(Date.now() + 7_200_000).toISOString(), lane: 'north' });
  editGateAppointment(appointment.id, { status: 'checked-in' });
  const task = createYardTask({ kind: 'receive', containerId: container.id, from: '闸口', to: 'C03-R12-T1', priority: 50, scheduledAt: new Date().toISOString(), estimatedMinutes: 15 });
  assignTask(task.id, 'YC-05');
  deleteContainer(container.id);
  unsubscribe();
  const types = seen.map(event => `${event.module}:${event.type}`);
  for (const expected of ['containers:container.created', 'containers:container.updated', 'containers:container.deleted', 'gate:appointment.created', 'gate:appointment.updated', 'gate:appointment.deleted', 'tasks:task.created', 'tasks:task.updated', 'tasks:task.deleted']) {
    assert.ok(types.includes(expected), `缺少事件 ${expected}`);
  }
  const created = seen.find(event => event.type === 'container.created');
  assert.equal(created?.entityId, 'TEST-EVT-0001');
  const taskCreated = seen.find(event => event.type === 'task.created');
  assert.equal(typeof (taskCreated?.data as { score?: number }).score, 'number');
  const ids = seen.map(event => event.id);
  assert.deepEqual(ids, [...ids].sort((a, b) => a - b));
});

test('snapshot builder only includes subscribed modules', async () => {
  const { snapshotModules } = await import('./domain.js');
  const snapshot = snapshotModules(new Set(['containers', 'gate']));
  assert.ok(Array.isArray(snapshot.containers));
  assert.ok(Array.isArray(snapshot.appointments));
  assert.ok(Array.isArray(snapshot.conflicts));
  assert.equal(snapshot.tasks, undefined);
  assert.equal(snapshot.dashboard, undefined);
});

test('telemetry tick publishes equipment, voyage, yard and reefer updates in the unified format', async () => {
  const { telemetryTick } = await import('./telemetry.js');
  const { opsBus } = await import('./events.js');
  const seen: OpsEvent[] = [];
  const unsubscribe = opsBus.subscribe(event => seen.push(event));
  telemetryTick(() => 0.5);
  unsubscribe();
  const types = seen.map(event => event.type);
  assert.ok(types.includes('equipment.updated'));
  assert.ok(types.includes('voyage.updated'));
  assert.ok(types.includes('yard.block.updated'));
  assert.ok(types.includes('container.updated'));
  for (const event of seen) {
    assert.ok(event.id > 0 && event.at && event.module && event.entityId);
  }
});
