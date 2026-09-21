import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { Injectable } from '@nestjs/common';

@Injectable()
export class WhatsappSyncQueue {
  constructor(@InjectQueue('whatsapp-sync') private readonly queue: Queue) {}

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

  async enqueueFullSync(sessionId: string) {
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
