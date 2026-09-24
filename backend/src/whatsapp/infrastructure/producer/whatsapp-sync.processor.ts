import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { Logger } from '@nestjs/common';
import { WhatsappSyncService } from 'src/whatsapp/application/whatsapp-sync.service';

@Processor('whatsapp-sync')
export class WhatsappSyncProcessor extends WorkerHost {
  private readonly logger = new Logger(WhatsappSyncProcessor.name);

  constructor(private readonly syncService: WhatsappSyncService) {
    super();
  }

  async process(job: Job<{ sessionId: string; notify?: boolean }>) {
    const { sessionId, notify } = job.data;
    try {
      await this.syncService.syncAll(sessionId, { notify: !!notify });
    } catch (err) {
      this.logger.error(`Sync falló para ${sessionId}: ${err?.message}`);
      throw err; // deja que BullMQ lo marque como failed/reintente si configuraste attempts
    }
  }
}
