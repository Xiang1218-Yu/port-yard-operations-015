import { blockHeatRow, blocks, containers, equipment, voyages } from './domain.js';
import { opsBus } from './events.js';

function clamp(value: number, min: number, max: number) { return Math.min(max, Math.max(min, value)); }

/**
 * 模拟 RTG/闸口/船期遥测：让没有 CRUD 入口的模块（设备、航次、堆场、冷藏温控）
 * 也持续产生统一格式的实时事件。后续接入真实 TOS/遥测时替换这里的数据源即可。
 */
export function telemetryTick(rand: () => number = Math.random) {
  const machine = equipment[Math.floor(rand() * equipment.length)];
  if (machine) {
    machine.battery = clamp(machine.battery + (machine.status === 'working' ? -1 - Math.floor(rand() * 3) : 1), 4, 100);
    machine.queue = clamp(machine.queue + (rand() > 0.5 ? 1 : -1), 0, 12);
    machine.movesPerHour = clamp(machine.movesPerHour + Math.round(rand() * 4 - 2), 4, 36);
    opsBus.publish('equipment', 'equipment.updated', machine, machine.id);
  }

  const active = voyages.filter(voyage => voyage.status === 'berthed' || voyage.status === 'loading');
  const voyage = active[Math.floor(rand() * active.length)];
  if (voyage) {
    voyage.movesDone = Math.min(voyage.movesPlanned, voyage.movesDone + 1 + Math.floor(rand() * 5));
    opsBus.publish('voyages', 'voyage.updated', voyage, voyage.id);
  }

  const block = blocks[Math.floor(rand() * blocks.length)];
  if (block) {
    block.heat = clamp(block.heat + Math.round(rand() * 6 - 3), 30, 98);
    block.occupied = clamp(block.occupied + (rand() > 0.5 ? 1 : -1), 0, block.capacity);
    opsBus.publish('yard', 'yard.block.updated', blockHeatRow(block), block.id);
  }

  const reefers = containers.filter(container => container.reefer && container.temperature !== undefined);
  const reefer = reefers[Math.floor(rand() * reefers.length)];
  if (reefer && rand() > 0.4) {
    reefer.temperature = Math.round((reefer.temperature! + rand() * 0.8 - 0.4) * 10) / 10;
    opsBus.publish('containers', 'container.updated', reefer, reefer.id);
  }
}

export function startTelemetry(intervalMs = 15_000) {
  const timer = setInterval(() => telemetryTick(), intervalMs);
  return () => clearInterval(timer);
}
