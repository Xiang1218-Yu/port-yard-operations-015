/**
 * 控制台实时同步层：SSE 连接管理 + 按模块订阅分发。
 *
 * - 断线指数退避重连，重连时携带最后事件游标
 * - 心跳超时判定连接僵死并主动重建
 * - 事件按游标去重；快照携带游标，与快照重叠的增量直接丢弃
 * - 游标过期时服务端下发 resync，客户端清空游标重新拿快照
 * - 单个订阅处理器异常被隔离，不影响其他模块
 */

export type Module = 'voyages' | 'containers' | 'yard' | 'gate' | 'tasks' | 'equipment' | 'alerts' | 'dashboard';

export interface OpsEvent<T = unknown> { id: number; module: Module; type: string; entityId?: string; at: string; data: T; }
export interface SnapshotPayload { seq: number; modules: Record<string, unknown>; }
export type ConnectionState = 'connecting' | 'open' | 'reconnecting';

type OpsHandler = (event: OpsEvent) => void;
type SnapshotHandler = (payload: SnapshotPayload) => void;
type StateHandler = (state: ConnectionState) => void;
type ErrorHandler = (error: unknown, event?: OpsEvent) => void;

export interface RealtimeOptions {
  /** 超过该时长没有任何消息（含心跳）判定连接僵死，默认 45s（服务端心跳 15s 的 3 倍） */
  heartbeatTimeoutMs?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

export class RealtimeClient {
  private source?: EventSource;
  private lastSeq = 0;
  private attempts = 0;
  private stopped = true;
  private state: ConnectionState = 'connecting';
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private staleTimer?: ReturnType<typeof setTimeout>;
  private readonly opsHandlers = new Map<Module, Set<OpsHandler>>();
  private readonly snapshotHandlers = new Set<SnapshotHandler>();
  private readonly stateHandlers = new Set<StateHandler>();
  private readonly errorHandlers = new Set<ErrorHandler>();
  private readonly heartbeatTimeoutMs: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;

  constructor(private readonly url = '/api/events', options: RealtimeOptions = {}) {
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 45_000;
    this.baseDelayMs = options.baseDelayMs ?? 500;
    this.maxDelayMs = options.maxDelayMs ?? 15_000;
  }

  get cursor() { return this.lastSeq; }
  get connectionState() { return this.state; }

  /** 按模块订阅事件；返回取消订阅函数。处理器异常会被隔离并上报到 onHandlerError */
  subscribe(module: Module, handler: OpsHandler): () => void {
    const handlers = this.opsHandlers.get(module) ?? new Set<OpsHandler>();
    handlers.add(handler);
    this.opsHandlers.set(module, handlers);
    return () => { handlers.delete(handler); };
  }

  onSnapshot(handler: SnapshotHandler): () => void { this.snapshotHandlers.add(handler); return () => { this.snapshotHandlers.delete(handler); }; }
  onState(handler: StateHandler): () => void { this.stateHandlers.add(handler); return () => { this.stateHandlers.delete(handler); }; }
  onHandlerError(handler: ErrorHandler): () => void { this.errorHandlers.add(handler); return () => { this.errorHandlers.delete(handler); }; }

  start() {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.staleTimer) clearTimeout(this.staleTimer);
    this.reconnectTimer = undefined;
    this.staleTimer = undefined;
    this.source?.close();
    this.source = undefined;
  }

  private connect() {
    if (this.stopped) return;
    this.source?.close();
    const params = new URLSearchParams();
    const modules = [...this.opsHandlers.keys()];
    if (modules.length) params.set('modules', modules.join(','));
    if (this.lastSeq > 0) params.set('lastEventId', String(this.lastSeq));
    const source = new EventSource(`${this.url}?${params.toString()}`);
    this.source = source;
    this.setState(this.attempts === 0 ? 'connecting' : 'reconnecting');
    source.onopen = () => {
      this.attempts = 0;
      this.setState('open');
      this.armStaleTimer();
    };
    source.addEventListener('ops', message => this.handleOps(message as MessageEvent));
    source.addEventListener('snapshot', message => this.handleSnapshot(message as MessageEvent));
    source.addEventListener('heartbeat', () => this.armStaleTimer());
    source.addEventListener('resync', () => this.handleResync());
    source.addEventListener('rebalance', () => this.scheduleReconnect());
    source.onerror = () => this.scheduleReconnect();
  }

  private handleOps(message: MessageEvent) {
    this.armStaleTimer();
    let event: OpsEvent;
    try {
      event = JSON.parse(message.data as string) as OpsEvent;
    } catch (error) {
      this.report(error);
      return;
    }
    // 重复事件 / 与快照重叠的增量：按游标去重
    if (event.id <= this.lastSeq) return;
    this.lastSeq = event.id;
    const handlers = this.opsHandlers.get(event.module);
    if (!handlers) return;
    for (const handler of [...handlers]) {
      try {
        handler(event);
      } catch (error) {
        // 单个模块的订阅异常不影响其他模块
        this.report(error, event);
      }
    }
  }

  private handleSnapshot(message: MessageEvent) {
    this.armStaleTimer();
    let payload: SnapshotPayload;
    try {
      payload = JSON.parse(message.data as string) as SnapshotPayload;
    } catch (error) {
      this.report(error);
      return;
    }
    // 快照游标之后到达的增量才生效，之前的视为已包含在快照中
    this.lastSeq = Math.max(this.lastSeq, payload.seq);
    for (const handler of [...this.snapshotHandlers]) {
      try {
        handler(payload);
      } catch (error) {
        this.report(error);
      }
    }
  }

  /** 游标过期：清空游标重连，服务端会重新下发完整快照 */
  private handleResync() {
    this.lastSeq = 0;
    this.scheduleReconnect();
  }

  private scheduleReconnect() {
    if (this.stopped || this.reconnectTimer) return;
    this.source?.close();
    this.source = undefined;
    if (this.staleTimer) {
      clearTimeout(this.staleTimer);
      this.staleTimer = undefined;
    }
    this.attempts += 1;
    const delay = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** (this.attempts - 1)) * (0.6 + Math.random() * 0.4);
    this.setState('reconnecting');
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
  }

  /** 心跳看门狗：长时间没有任何消息说明连接已僵死（TCP 半开），主动重建 */
  private armStaleTimer() {
    if (this.staleTimer) clearTimeout(this.staleTimer);
    this.staleTimer = setTimeout(() => {
      this.staleTimer = undefined;
      this.scheduleReconnect();
    }, this.heartbeatTimeoutMs);
  }

  private setState(state: ConnectionState) {
    if (this.state === state) return;
    this.state = state;
    for (const handler of [...this.stateHandlers]) {
      try {
        handler(state);
      } catch { /* 状态监听异常隔离 */ }
    }
  }

  private report(error: unknown, event?: OpsEvent) {
    if (!this.errorHandlers.size) console.error('[realtime] 订阅处理器异常已隔离', event?.type, error);
    for (const handler of [...this.errorHandlers]) {
      try {
        handler(error, event);
      } catch { /* 错误回调异常隔离 */ }
    }
  }
}
