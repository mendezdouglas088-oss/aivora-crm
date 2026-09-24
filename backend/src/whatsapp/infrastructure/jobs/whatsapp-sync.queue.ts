import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { Injectable } from '@nestjs/common';

@Injectable()
export class WhatsappSyncQueue {
  constructor(@InjectQueue('whatsapp-sync') private readonly queue: Queue) {}

  /**
   * Refresco en background (GET /chats, GET /groups, sync periódico).
   * NO notifica por WebSocket: si lo hiciera, un frontend que reacciona a
   * `chats-synced` volviendo a pedir GET /chats encolaría otro sync que
   * emitiría de nuevo... un bucle sin fin.
   */
  async enqueueSync(sessionId: string) {
    // jobId fijo: si ya hay un sync de esta conexión esperando/en curso, BullMQ
    // ignora el nuevo. Antes cada GET /chats y /groups apilaba otro sync-all.
    // removeOnFail: true — un job fallido con el mismo id bloquearía los siguientes.
    await this.queue.add(
      'sync-all',
      { sessionId },
      {
        jobId: `sync-${sessionId}`,
        attempts: 4,
        backoff: { type: 'exponential', delay: 5000 },
        removeOnComplete: true,
        removeOnFail: true,
      },
    );
  }

  /**
   * Sync completo QUE NOTIFICA: carga primero los chats y emite
   * `whatsapp:chats-synced`, luego los grupos y emite `whatsapp:groups-synced`.
   * Se usa al conectar, al terminar de importar el historial, en
   * POST /whatsapp/sync y en GET /whatsapp/update-groups.
   * jobId distinto al del refresco en background para que un refresco ya
   * encolado no "absorba" este y se pierdan las notificaciones.
   */
  async enqueueFullSync(sessionId: string) {
    await this.queue.add(
      'sync-all',
      { sessionId, notify: true },
      {
        jobId: `sync-full-${sessionId}`,
        attempts: 4,
        backoff: { type: 'exponential', delay: 5000 },
        removeOnComplete: true,
        removeOnFail: true,
      },
    );
  }

  // sync periódico mientras el cliente esté conectado
  async scheduleRecurringSync(sessionId: string) {
    await this.queue.upsertJobScheduler(
      `recurring-${sessionId}`, // ID estable del scheduler
      { every: 3 * 60 * 1000 }, // cada 3 min
      {
        name: 'sync-all',
        data: { sessionId },
        opts: { removeOnComplete: true },
      },
    );
  }

  async stopRecurringSync(sessionId: string) {
    await this.queue.removeJobScheduler(`recurring-${sessionId}`);
  }
}
