import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import Redis from 'ioredis';
import { redisOptions } from 'src/config/bullmq.config';
import {
  WHATSAPP_LIVE_EVENTS_CHANNEL,
  WhatsappLiveEvent,
} from 'shared/whatsapp-contracts';

/**
 * Se suscribe al canal de Redis que usa whatsapp-runtime.service.ts (otro
 * proceso) y reemite cada evento como el mismo `@OnEvent(...)` local que ya
 * escuchaban whatsapp-events.listener.ts y realtime.gateway.ts. Por eso
 * ninguno de esos dos archivos necesitó tocarse para el 'qr'/'status'/
 * 'call'/'message-received'.
 *
 * OJO con los nombres de campo: el contrato compartido estandarizó
 * `connectionId` en todos los eventos, pero `realtime.gateway.ts` (que no
 * se tocó) espera `sessionId` en los handlers de 'call' y
 * 'message.received' — es justo el trabajo de este bridge traducirlo.
 *
 * Usa una conexión de Redis DEDICADA (no la de BullMQ ni la de
 * WhatsappCommandsService) porque SUBSCRIBE pone al cliente en modo
 * exclusivo: una vez suscrito, esa conexión no puede usarse para nada más.
 */
@Injectable()
export class WhatsappLiveEventsBridge implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WhatsappLiveEventsBridge.name);
  private readonly subscriber = new Redis(redisOptions);

  constructor(private readonly eventEmitter: EventEmitter2) {}

  async onModuleInit() {
    await this.subscriber.subscribe(WHATSAPP_LIVE_EVENTS_CHANNEL);
    this.subscriber.on('message', (_channel, message) => {
      this.handleMessage(message);
    });
    this.logger.log(`Suscrito a Redis "${WHATSAPP_LIVE_EVENTS_CHANNEL}"`);
  }

  async onModuleDestroy() {
    await this.subscriber.unsubscribe(WHATSAPP_LIVE_EVENTS_CHANNEL);
    this.subscriber.disconnect();
  }

  private handleMessage(raw: string) {
    let event: WhatsappLiveEvent;
    try {
      event = JSON.parse(raw);
    } catch (e) {
      this.logger.warn(`Evento en vivo con JSON inválido: ${e.message}`);
      return;
    }

    switch (event.kind) {
      case 'qr':
        this.eventEmitter.emit('whatsapp.qr', {
          connectionId: event.connectionId,
          qr: event.qr,
        });
        break;

      case 'status':
        this.eventEmitter.emit('whatsapp.status', {
          connectionId: event.connectionId,
          status: event.status,
        });
        break;

      case 'call':
        // realtime.gateway.ts espera `sessionId` para este evento puntual
        this.eventEmitter.emit('whatsapp.call', {
          sessionId: event.connectionId,
          from: event.from,
          isVideo: event.isVideo,
          isGroup: event.isGroup,
          timestamp: event.timestamp,
        });
        break;

      case 'message-received':
        // ídem: realtime.gateway.ts hace `payload.sessionId` para la sala
        this.eventEmitter.emit('whatsapp.message.received', {
          sessionId: event.connectionId,
          chatId: event.chatId,
          isGroup: event.isGroup,
          contact: event.contact,
          text: event.text,
          messageId: event.messageId,
          serializedId: event.serializedId,
          type: event.type,
          hasMedia: event.hasMedia,
        });
        break;

      default: {
        const exhaustiveCheck: never = event;
        this.logger.warn(
          `Evento en vivo desconocido: ${(exhaustiveCheck as WhatsappLiveEvent).kind}`,
        );
      }
    }
  }
}
