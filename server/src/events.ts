import type { Request, Response } from 'express';

export type OpsModule = 'voyages' | 'containers' | 'appointments' | 'tasks' | 'equipment' | 'yard' | 'alerts' | 'dashboard';
export const ALL_MODULES: OpsModule[] = ['voyages', 'containers', 'appointments', 'tasks', 'equipment', 'yard', 'alerts', 'dashboard'];

export interface OpsEvent<T = unknown> {
  id: number;               // 全局单调递增游标，断线续传与去重的基准
  module: OpsModule;
  type: 'snapshot' | 'upsert' | 'delete';
  entityId: string;
  data?: T;
  at: string;
}

const LOG_LIMIT = 1000;      // 事件日志环形缓冲，供 Last-Event-ID 重放
const HEARTBEAT_MS = 15_000; // 心跳间隔，防止代理断链并用于探活
const SWEEP_MS = 30_000;     // 空闲连接巡检周期
const IDLE_LIMIT_MS = 90_000;// 超过该时长写不进去的连接判定为僵尸连接

/** 进程内事件总线：发布即入日志并广播，单个订阅者异常不影响其他订阅者。 */
class EventBus {
  private seq = 0;
  private log: OpsEvent[] = [];
  private listeners = new Set<(event: OpsEvent) => void>();

  currentSeq() { return this.seq; }
  oldestSeq() { return this.log.length ? this.log[0].id : this.seq; }

  publish<T>(module: OpsModule, type: OpsEvent['type'], entityId: string, data?: T): OpsEvent<T> {
    const event: OpsEvent<T> = { id: ++this.seq, module, type, entityId, data, at: new Date().toISOString() };
    this.log.push(event);
    if (this.log.length > LOG_LIMIT) this.log.splice(0, this.log.length - LOG_LIMIT);
    for (const listener of [...this.listeners]) {
      try { listener(event); } catch { /* 订阅者异常隔离：不中断总线 */ }
    }
    return event;
  }

  since(cursor: number): OpsEvent[] { return this.log.filter(event => event.id > cursor); }

  subscribe(listener: (event: OpsEvent) => void) {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }
}

export const bus = new EventBus();

interface SseClient {
  res: Response;
  modules: Set<OpsModule>;
  lastWrite: number;
}

const clients = new Set<SseClient>();

function writeEvent(res: Response, event: OpsEvent) {
  res.write(`id: ${event.id}\nevent: ops\ndata: ${JSON.stringify(event)}\n\n`);
}

// 订阅即写入；写失败只移除该客户端，不影响其他连接
bus.subscribe(event => {
  for (const client of [...clients]) {
    if (!client.modules.has(event.module)) continue;
    try { writeEvent(client.res, event); client.lastWrite = Date.now(); }
    catch { dropClient(client); }
  }
});

function dropClient(client: SseClient) {
  clients.delete(client);
  try { client.res.end(); } catch { /* 已断开 */ }
}

// 心跳 + 僵尸连接清理：心跳写不进去或长时间无任何成功写入的连接被回收
setInterval(() => {
  for (const client of [...clients]) {
    try { client.res.write(`:hb ${Date.now()}\n\n`); client.lastWrite = Date.now(); }
    catch { dropClient(client); }
  }
}, HEARTBEAT_MS).unref();

setInterval(() => {
  const now = Date.now();
  for (const client of [...clients]) {
    if (client.res.writableEnded || client.res.destroyed || now - client.lastWrite > IDLE_LIMIT_MS) dropClient(client);
  }
}, SWEEP_MS).unref();

export function sseClientCount() { return clients.size; }

/**
 * SSE 入口。支持：
 * - ?modules=a,b,c 按模块订阅（缺省全量）
 * - ?cursor=N 或 Last-Event-ID 断点续传：游标仍在日志窗口内时只重放增量
 * - 游标缺失或已过期：先发全模块快照（快照事件共享拍摄时刻的游标），再补游标之后的增量，
 *   快照与增量之间新产生的事件既会进日志重放、也会经订阅实时写入，客户端按事件 id 去重。
 */
export function eventsHandler(snapshot: (modules: Set<OpsModule>) => { module: OpsModule; entityId: string; data: unknown }[]) {
  return (req: Request, res: Response) => {
    const requested = String(req.query.modules ?? '').split(',').map(item => item.trim()).filter(Boolean);
    const modules = new Set<OpsModule>(requested.length ? requested.filter((item): item is OpsModule => ALL_MODULES.includes(item as OpsModule)) : ALL_MODULES);
    const cursor = Number(req.query.cursor ?? req.header('Last-Event-ID') ?? 0) || 0;

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.flushHeaders();

    const client: SseClient = { res, modules, lastWrite: Date.now() };
    clients.add(client);
    req.on('close', () => clients.delete(client));

    const canResume = cursor > 0 && cursor >= bus.oldestSeq() - 1;
    let from = cursor;
    if (!canResume) {
      // 全量同步：先拍快照（游标取拍摄时刻的全局序号），之后只放行比快照更新的增量
      const snapshotCursor = bus.currentSeq();
      for (const item of snapshot(modules)) {
        writeEvent(res, { id: snapshotCursor, module: item.module, type: 'snapshot', entityId: item.entityId, data: item.data, at: new Date().toISOString() });
      }
      res.write(`event: ready\ndata: {"cursor":${snapshotCursor}}\n\n`);
      from = snapshotCursor;
    }
    for (const event of bus.since(from)) {
      if (client.modules.has(event.module)) writeEvent(res, event);
    }
    client.lastWrite = Date.now();
  };
}
