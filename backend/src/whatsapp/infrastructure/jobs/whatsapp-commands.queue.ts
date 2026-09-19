import { InjectQueue } from '@nestjs/bullmq';
import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Queue, QueueEvents } from 'bullmq';
import { bullConfig } from 'src/config/bullmq.config';
import {
  WHATSAPP_COMMAND_JOB_NAME,
  WHATSAPP_COMMANDS_QUEUE,
  WhatsappCommand,
  WhatsappCommandResult,
} from 'shared/whatsapp-contracts';

const DEFAULT_TIMEOUT_MS = 30_000;

interface SendOptions {
  /** Cuánto espera la API antes de darse por vencida con este comando. */
  timeoutMs?: number;
  /**
   * Cuántas veces reintenta BullMQ si el runtime falla al procesar el comando.
   * Por defecto 1 (sin reintento): para comandos que ENVÍAN algo (send-text,
   * send-images, send-media) reintentar automáticamente podría duplicar un
   * mensaje que en realidad sí llegó a WhatsApp pero cuya respuesta se perdió
   * en el camino. Para comandos de lectura o idempotentes (connect, logout,
   * sync-all, get-media, get-contact) sí conviene pasar attempts > 1.
   */
  attempts?: number;
}

@Injectable()
export class WhatsappCommandsQueue implements OnModuleDestroy {
  private readonly queueEvents: QueueEvents;

  constructor(
    @InjectQueue(WHATSAPP_COMMANDS_QUEUE) private readonly queue: Queue,
  ) {
    // QueueEvents necesita su propia conexión a Redis, no puede compartir
    // la del Queue — así lo pide BullMQ para poder escuchar los eventos
    // 'completed'/'failed' que hacen posible waitUntilFinished().
    this.queueEvents = new QueueEvents(WHATSAPP_COMMANDS_QUEUE, {
      connection: bullConfig.connection,
    });
  }

  async onModuleDestroy() {
    await this.queueEvents.close();
  }

  /**
   * Encola un comando y espera su resultado antes de devolver el control.
   * Es el reemplazo directo de `this.provider.xxx()` de antes — desde el
   * punto de vista de quien lo llama, se comporta igual (una promesa que
   * resuelve con el resultado), solo que por debajo cruza a otro proceso.
   */
  async send<T extends WhatsappCommand>(
    command: T,
    opts: SendOptions = {},
  ): Promise<WhatsappCommandResult<T['type']>> {
    const { timeoutMs = DEFAULT_TIMEOUT_MS, attempts = 1 } = opts;

    const job = await this.queue.add(WHATSAPP_COMMAND_JOB_NAME, command, {
      attempts,
      backoff: attempts > 1 ? { type: 'exponential', delay: 3000 } : undefined,
      removeOnComplete: true,
      removeOnFail: 200,
    });

    const result = await job.waitUntilFinished(this.queueEvents, timeoutMs);
    return result as WhatsappCommandResult<T['type']>;
  }
}
