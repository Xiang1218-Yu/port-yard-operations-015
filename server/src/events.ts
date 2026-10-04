/**
 * 实时事件总线与 SSE 连接管理。
 *
 * 统一事件格式：单调递增游标 id + 模块 + 类型 + 负载。
 * - 环形缓冲保留最近事件，支持断线后按游标回放
 * - 订阅按模块过滤，单个订阅者异常被隔离
 * - 心跳保活 + 空闲/积压/超龄连接清理
 */

export type Module = 'voyages' | 'containers' | 'yard' | 'gate' | 'tasks' | 'equipment' | 'alerts' | 'dashboard';
export const ALL_MODULES: readonly Module[] = ['voyages', 'containers', 'yard', 'gate', 'tasks', 'equipment', 'alerts', 'dashboard'];

export interface OpsEvent<T = unknown> {
  id: number;
  module: Module;
  type: string;
  entityId?: string;
  at: string;
  data: T;
}

type Listener = (event: OpsEvent) => void;

export class EventBus {
  private seq = 0;
  private buffer: OpsEvent[] = [];
  private readonly listeners = new Set<Listener>();

  constructor(private readonly capacity = 1000, private readonly clock: () => Date = () => new Date()) {}

  /** 当前游标：已发布事件的最大 id */
  get cursor() { return this.seq; }
  /** 缓冲区中仍可回放的最小事件 id */
  get oldest() { return this.buffer.length ? this.buffer[0]!.id : this.seq + 1; }

  publish<T>(module: Module, type: string, data: T, entityId?: string): OpsEvent<T> {
    const event: OpsEvent<T> = { id: ++this.seq, module, type, entityId, at: this.clock().toISOString(), data };
    this.buffer.push(event);
    if (this.buffer.length > this.capacity) this.buffer.splice(0, this.buffer.length - this.capacity);
    for (const listener of [...this.listeners]) {
      try {
        listener(event);
      } catch (error) {
        // 单个订阅者异常不影响其他订阅者
        console.error(`[events] 订阅者处理 ${type} 异常，已隔离`, error);
      }
    }
    return event;
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** 游标之后的事件是否都还在缓冲区内（能否无损回放） */
  covers(lastEventId: number): boolean { return lastEventId >= this.oldest - 1; }

  replay(afterId: number, modules: ReadonlySet<Module>): OpsEvent[] {
    return this.buffer.filter(event => event.id > afterId && modules.has(event.module));
  }
}

/** 全局作业事件总线：领域变更、遥测与派生视图都发布到这里 */
export const opsBus = new EventBus();

/** 最小 SSE 输出端抽象，express Response 天然满足该结构，测试可用假 sink */
export interface SseSink {
  write(chunk: string): unknown;
  end(): unknown;
  readonly writableLength?: number;
  readonly destroyed?: boolean;
}

export interface StreamParams { modules?: string; lastEventId?: string | number; }

export interface StreamHubOptions {
  heartbeatMs?: number;
  sweepMs?: number;
  /** 距上次成功写入超过该时长视为僵死连接 */
  staleMs?: number;
  /** 单连接积压字节上限，超过即断开（慢客户端保护） */
  maxBufferedBytes?: number;
  /** 单连接最大存活时长，到期让客户端重连再均衡 */
  maxAgeMs?: number;
  now?: () => number;
}

const DEFAULTS = {
  heartbeatMs: 15_000,
  sweepMs: 30_000,
  staleMs: 90_000,
  maxBufferedBytes: 1_000_000,
  maxAgeMs: 30 * 60_000,
};

interface Connection {
  id: string;
  sink: SseSink;
  modules: ReadonlySet<Module>;
  connectedAt: number;
  lastWriteAt: number;
  closed: boolean;
}

export function formatSse(event: string, data: unknown, id?: number): string {
  // 心跳等控制帧不带 id，游标只能由真实事件推进
  const head = id === undefined ? `event: ${event}\n` : `event: ${event}\nid: ${id}\n`;
  return `${head}data: ${JSON.stringify(data)}\n\n`;
}

export function parseModules(raw?: string): Set<Module> {
  if (!raw || raw === 'all') return new Set(ALL_MODULES);
  const requested = raw.split(',').map(item => item.trim()).filter((item): item is Module => (ALL_MODULES as readonly string[]).includes(item));
  return requested.length ? new Set(requested) : new Set(ALL_MODULES);
}

export class StreamHub {
  private readonly connections = new Map<string, Connection>();
  private readonly options: typeof DEFAULTS & { now: () => number };
  private readonly unsubscribe: () => void;
  private timers: ReturnType<typeof setInterval>[] = [];
  private nextId = 0;

  constructor(
    private readonly bus: EventBus,
    private readonly buildSnapshot: (modules: ReadonlySet<Module>) => Record<string, unknown>,
    options: StreamHubOptions = {},
  ) {
    this.options = { ...DEFAULTS, ...options, now: options.now ?? (() => Date.now()) };
    this.unsubscribe = bus.subscribe(event => this.fanout(event));
  }

  get size() { return this.connections.size; }

  start() {
    if (this.timers.length) return;
    this.timers = [
      setInterval(() => this.heartbeat(), this.options.heartbeatMs),
      setInterval(() => this.sweep(), this.options.sweepMs),
    ];
  }

  stop() {
    this.timers.forEach(clearInterval);
    this.timers = [];
    for (const id of [...this.connections.keys()]) this.detach(id);
    this.unsubscribe();
  }

  /**
   * 接入一条 SSE 连接。
   * - 带有效游标：只回放游标之后的增量；游标太旧（已被挤出缓冲区）则发 resync 让客户端全量重建
   * - 无游标：先记录游标再取快照，快照之后回放期间产生的增量，保证快照与增量交错时不丢不重
   */
  attach(sink: SseSink, params: StreamParams): string {
    const id = `conn-${++this.nextId}`;
    const modules = parseModules(params.modules);
    const now = this.options.now();
    const connection: Connection = { id, sink, modules, connectedAt: now, lastWriteAt: now, closed: false };

    const cursor = Number(params.lastEventId);
    if (Number.isFinite(cursor) && cursor > 0) {
      this.connections.set(id, connection);
      if (!this.bus.covers(cursor)) {
        this.write(connection, formatSse('resync', { reason: 'cursor-expired', oldest: this.bus.oldest }));
        this.detach(id);
        return id;
      }
      for (const event of this.bus.replay(cursor, modules)) this.sendEvent(connection, event);
      return id;
    }

    const seq = this.bus.cursor;
    const snapshot = this.buildSnapshot(modules);
    this.write(connection, formatSse('snapshot', { seq, modules: snapshot }, seq));
    // 快照构造期间产生的增量已进入缓冲区，在注册连接前取出，随快照之后按序补发
    const missed = this.bus.replay(seq, modules);
    this.connections.set(id, connection);
    for (const event of missed) this.sendEvent(connection, event);
    return id;
  }

  detach(id: string) {
    const connection = this.connections.get(id);
    if (!connection || connection.closed) return;
    connection.closed = true;
    this.connections.delete(id);
    try { connection.sink.end(); } catch { /* 关闭异常忽略 */ }
  }

  /** 心跳：保活 NAT/代理，并让半开连接在写失败时被及时发现清理 */
  heartbeat() {
    const seq = this.bus.cursor;
    for (const connection of [...this.connections.values()]) {
      this.write(connection, formatSse('heartbeat', { seq, at: new Date(this.options.now()).toISOString() }));
    }
  }

  /** 清理长时间空闲（写入停滞）、积压过多或存活过久的连接 */
  sweep() {
    const now = this.options.now();
    for (const connection of [...this.connections.values()]) {
      const buffered = connection.sink.writableLength ?? 0;
      if (connection.sink.destroyed || now - connection.lastWriteAt > this.options.staleMs || buffered > this.options.maxBufferedBytes) {
        this.detach(connection.id);
      } else if (now - connection.connectedAt > this.options.maxAgeMs) {
        this.write(connection, formatSse('rebalance', { reason: 'max-age' }));
        this.detach(connection.id);
      }
    }
  }

  private fanout(event: OpsEvent) {
    for (const connection of [...this.connections.values()]) {
      if (connection.closed || !connection.modules.has(event.module)) continue;
      this.sendEvent(connection, event);
    }
  }

  private sendEvent(connection: Connection, event: OpsEvent) {
    this.write(connection, formatSse('ops', event, event.id));
  }

  private write(connection: Connection, chunk: string) {
    if (connection.closed) return;
    try {
      connection.sink.write(chunk);
      connection.lastWriteAt = this.options.now();
    } catch (error) {
      // 单条连接写失败只清理该连接，不影响其他连接
      console.error(`[events] 连接 ${connection.id} 写入失败，已清理`, error);
      this.detach(connection.id);
    }
  }
}
