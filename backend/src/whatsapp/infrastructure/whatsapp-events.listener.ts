import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { WhatsappSyncQueue } from './jobs/whatsapp-sync.queue';
import { WhatsappConnectionsService } from '../services/whatsapp-connections.service';
import { WhatsappConnectionStatus } from 'shared/whatsapp-contracts';

/**
 * Antes también tenía el handler de 'whatsapp.message.persist' — se movió a
 * whatsapp-persist-events.processor.ts porque ese evento ahora cruza de
 * proceso (runtime -> API) y necesita ir por una cola BullMQ con reintentos,
 * no un simple @OnEvent en memoria.
 *
 * Este listener sigue escuchando 'whatsapp.status', pero ahora ese evento
 * lo produce whatsapp-live-events.bridge.ts (reenviándolo desde Redis
 * Pub/Sub), no el emit local que hacía whatsapp-web.provider.ts antes.
 */
@Injectable()
export class WhatsappEventsListener {
  constructor(
    private readonly syncQueue: WhatsappSyncQueue,
    private readonly connectionsService: WhatsappConnectionsService,
  ) {}

  @OnEvent('whatsapp.status')
  async handleStatus(payload: {
    connectionId: string;
    status: WhatsappConnectionStatus;
  }) {
    // Nuevo: se refleja el estado en la BD (columna agregada en la Fase 1)
    await this.connectionsService.updateStatus(
      payload.connectionId,
      payload.status,
    );

    if (payload.status === 'connected') {
      await this.syncQueue.enqueueSync(payload.connectionId);
      await this.syncQueue.scheduleRecurringSync(payload.connectionId);
    }
    if (payload.status === 'disconnected') {
      await this.syncQueue.stopRecurringSync(payload.connectionId);
    }
  }
}
