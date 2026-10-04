export type OpsModule = 'voyages' | 'containers' | 'appointments' | 'tasks' | 'equipment' | 'yard' | 'alerts' | 'dashboard';
export interface OpsEvent<T = unknown> {
  id: number;
  module: OpsModule;
  type: 'snapshot' | 'upsert' | 'delete';
  entityId: string;
  data?: T;
  at: string;
}
export type ConnectionStatus = 'connecting' | 'open' | 'retrying' | 'closed';
type Handler = (event: OpsEvent) => void;

const MAX_BACKOFF_MS = 15_000;
const STALE_MS = 45_000; // 服务端心跳 15s，超过 45s 无任何消息判定连接僵死

/**
 * 控制台实时同步层：
 * - 按模块订阅，模块集合变化时自动重连协商
 * - 断线指数退避重连，重连携带最后事件游标（cursor），服务端只补增量
 * - 事件 id 去重：快照与增量交错、重放与实时推送重叠时不会重复应用
 * - 单个模块订阅者抛异常会被隔离并上报，不影响其他模块分发
 */
export class RealtimeClient {
  private handlers = new Map<OpsModule, Set<Handler>>();
  private source?: EventSource;
  private watchdog?: number;
  private retryTimer?: number;
  private attempts = 0;
  private lastId = 0;
  private recentIds: number[] = [];
  private lastMessageAt = 0;
  private disposed = false;
  onStatus?: (status: ConnectionStatus, lastEventId: number) => void;
  onHandlerError?: (module: OpsModule, error: unknown) => void;

  status: ConnectionStatus = 'closed';

  get cursor() { return this.lastId; }

  subscribe(module: OpsModule, handler: Handler) {
    let set = this.handlers.get(module);
    if (!set) { set = new Set(); this.handlers.set(module, set); }
    set.add(handler);
    this.reconnect(); // 模块集合可能变化，重新协商订阅
    return () => {
      const current = this.handlers.get(module);
      current?.delete(handler);
      if (current && !current.size) this.handlers.delete(module);
    };
  }

  private setStatus(status: ConnectionStatus) {
    this.status = status;
    this.onStatus?.(status, this.lastId);
  }

  reconnect() {
    if (this.disposed) return;
    this.source?.close();
    window.clearTimeout(this.retryTimer);
    if (!this.handlers.size) { this.setStatus('closed'); return; }
    const modules = [...this.handlers.keys()].join(',');
    const source = new EventSource(`/api/events?modules=${modules}&cursor=${this.lastId}`);
    this.source = source;
    this.setStatus('connecting');
    source.onopen = () => { this.attempts = 0; this.lastMessageAt = Date.now(); this.setStatus('open'); };
    source.addEventListener('ops', message => {
      this.lastMessageAt = Date.now();
      try { this.dispatch(JSON.parse(message.data) as OpsEvent); }
      catch (error) { console.error('[realtime] 无法解析事件', error); }
    });
    source.onerror = () => {
      source.close();
      if (this.source === source) this.scheduleRetry();
    };
    this.armWatchdog();
  }

  private scheduleRetry() {
    if (this.disposed || !this.handlers.size) return;
    this.setStatus('retrying');
    const delay = Math.min(MAX_BACKOFF_MS, 1000 * 2 ** this.attempts++);
    window.clearTimeout(this.retryTimer);
    this.retryTimer = window.setTimeout(() => this.reconnect(), delay);
  }

  private armWatchdog() {
    window.clearInterval(this.watchdog);
    this.watchdog = window.setInterval(() => {
      if (this.status === 'open' && Date.now() - this.lastMessageAt > STALE_MS) this.reconnect();
    }, 5_000);
  }

  private dispatch(event: OpsEvent) {
    if (event.type !== 'snapshot') {
      // 去重：游标单调递增，重复或乱序到达的旧事件直接丢弃
      if (event.id <= this.lastId || this.recentIds.includes(event.id)) return;
      this.recentIds.push(event.id);
      if (this.recentIds.length > 500) this.recentIds.splice(0, this.recentIds.length - 500);
    }
    this.lastId = Math.max(this.lastId, event.id);
    const set = this.handlers.get(event.module);
    if (!set) return;
    for (const handler of [...set]) {
      try { handler(event); }
      catch (error) { this.onHandlerError?.(event.module, error); console.error(`[realtime] 模块 ${event.module} 订阅者异常（已隔离）`, error); }
    }
    this.onStatus?.(this.status, this.lastId);
  }

  dispose() {
    this.disposed = true;
    this.source?.close();
    window.clearTimeout(this.retryTimer);
    window.clearInterval(this.watchdog);
    this.handlers.clear();
    this.setStatus('closed');
  }
}

/** 列表型模块的通用归约：快照整体替换，upsert 按 id 更新/前置，delete 移除。 */
export function applyEntityEvent<T extends { id: string }>(list: T[], event: OpsEvent, map?: (data: unknown) => T): T[] {
  const project = map ?? ((data: unknown) => data as T);
  if (event.type === 'snapshot') return (event.data as unknown[]).map(project);
  if (event.type === 'delete') return list.filter(item => item.id !== event.entityId);
  const next = project(event.data);
  return list.some(item => item.id === next.id) ? list.map(item => (item.id === next.id ? next : item)) : [next, ...list];
}
