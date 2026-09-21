import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import * as QRCode from 'qrcode';
import {
  SendResultInterface,
  WhatsappCallEvent,
  WhatsappChatSummary,
  WhatsappContact,
  WhatsappConnectionStatus,
  WhatsappMediaPayload,
  WhatsappMessagePersistPayload,
  WhatsappMessageReceivedEvent,
  WhatsappRawMessage,
  WhatsappLiveEvent,
  WhatsappPersistEventJob,
  WhatsappQrCacheValue,
  WHATSAPP_PERSIST_EVENTS_QUEUE,
  WHATSAPP_PERSIST_EVENT_JOB_NAME,
  WHATSAPP_LIVE_EVENTS_CHANNEL,
  whatsappStatusKey,
  whatsappQrKey,
} from '../../shared/whatsapp-contracts';
import { redisOptions } from '../../src/config/bullmq.config';
import {
  WhatsappBaileysEventListener,
  WhatsappBaileysProvider,
} from './whatsapp-baileys.provider';

interface RuntimeSession {
  status: WhatsappConnectionStatus;
  /** Timer del heartbeat que refresca `wa:status:*` mientras siga 'connected'. */
  heartbeatInterval?: NodeJS.Timeout;
}

/**
 * Estados que, si se encuentran en Redis al arrancar el proceso, son por
 * definición huérfanos: recién arranca, así que `this.sessions` está vacío
 * y no puede haber ningún socket vivo detrás de ellos todavía.
 */
const STALE_STATUSES_ON_BOOT: WhatsappConnectionStatus[] = [
  'connected',
  'connecting',
  'waiting_qr',
];

/**
 * TTL de la clave 'connected' en Redis y frecuencia con la que el heartbeat
 * la refresca. Si el proceso muere o el socket se cae sin que el provider
 * llegue a avisar 'disconnected' (ej. proceso matado por OOM), la clave
 * expira sola en vez de quedar diciendo 'connected' para siempre.
 */
const STATUS_HEARTBEAT_TTL_SECONDS = 60;

/** TTL de 'waiting_qr' y del PNG del QR; el provider los renueva con cada QR nuevo (~20 s). */
const QR_TTL_SECONDS = 90;
const STATUS_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * --- Migración whatsapp-web.js → Baileys ---
 * Esta clase mantiene exactamente el mismo rol que tenía antes: orquestar
 * Redis (status/QR/heartbeat) y las colas de BullMQ (comandos entrantes,
 * eventos de persistencia salientes). Lo único que cambió es DE DÓNDE
 * vienen los eventos y A QUIÉN se le delegan las operaciones de protocolo:
 *
 *  1. Ya no se crea ni se toca ningún `Client` acá — todo el detalle de
 *     Baileys (socket, mapeo de mensajes, envío, media, JIDs) vive en
 *     `WhatsappBaileysProvider`. Esta clase solo lo llama.
 *  2. Esta clase implementa `WhatsappBaileysEventListener`: el provider le
 *     avisa qr/status/call/mensajes por esos métodos en vez de que este
 *     archivo escuche `client.on(...)` directamente (que es justo lo que
 *     hacía la versión con whatsapp-web.js en `initClientForConnection`).
 *  3. `RemoteAuth` (Store de whatsapp-web.js, inyectado antes acá como
 *     `WhatsappRemoteAuthStore`) desapareció de este archivo: el auth state
 *     de Baileys (`WhatsappBaileysAuthStore`) se inyecta directo en el
 *     provider, no acá.
 *
 * Se mantiene sin cambios: reconciliación de estados huérfanos al
 * arrancar (`reconcileStaleStatuses`), TTL + heartbeat de 'connected', la
 * cola de persistencia y el canal de eventos en vivo — y por lo tanto,
 * tampoco cambió nada en `whatsapp-commands.processor.ts` (sigue llamando
 * a los mismos métodos públicos de esta clase) ni en el lado API
 * (`whatsapp-live-events.bridge.ts`, `whatsapp-persist-events.processor.ts`,
 * etc., que consumen Redis/BullMQ y no conocen ni a whatsapp-web.js ni a
 * Baileys).
 */
@Injectable()
export class WhatsappRuntimeService
  implements OnModuleInit, OnModuleDestroy, WhatsappBaileysEventListener
{
  private readonly logger = new Logger(WhatsappRuntimeService.name);
  private readonly sessions = new Map<string, RuntimeSession>();
  private readonly redis: Redis;

  constructor(
    @InjectQueue(WHATSAPP_PERSIST_EVENTS_QUEUE)
    private readonly persistQueue: Queue,
    private readonly provider: WhatsappBaileysProvider,
  ) {
    this.redis = new Redis(redisOptions);
  }

  async onModuleInit() {
    await this.reconcileStaleStatuses();
    this.logger.log(
      'Runtime de WhatsApp (Baileys) listo — las sesiones se crean bajo demanda.',
    );
  }

  async onModuleDestroy() {
    for (const session of this.sessions.values()) {
      this.stopHeartbeat(session);
    }
    await this.provider.destroyAll();
    this.redis.disconnect();
  }

  // ── contrato que espera WhatsappCommandsProcessor (sin cambios) ────

  async connect(connectionId: string): Promise<void> {
    const existing = this.sessions.get(connectionId);
    if (
      existing &&
      ['connecting', 'waiting_qr', 'connected'].includes(existing.status) &&
      // No fiarse solo del estado en memoria: si el socket ya no existe
      // (reintento fallido, cierre sin aviso) hay que volver a crearlo, en
      // vez de quedarse para siempre en 'waiting_qr'/'connecting'.
      this.provider.hasSession(connectionId)
    ) {
      return; // idempotente: seguro de reintentar
    }

    const session: RuntimeSession = existing ?? { status: 'disconnected' };
    this.sessions.set(connectionId, session);
    session.status = 'connecting';
    await this.redis.set(whatsappStatusKey(connectionId), 'connecting');

    try {
      await this.provider.connect(connectionId, this);
    } catch (e) {
      session.status = 'error';
      await this.redis.set(whatsappStatusKey(connectionId), 'error');
      await this.publishLiveEvent({
        kind: 'status',
        connectionId,
        status: 'error',
      });
      this.logger.error(
        `[${connectionId}] error al inicializar WhatsApp: ${e.message}`,
      );
    }
  }

  getStatus(connectionId: string): WhatsappConnectionStatus {
    return this.sessions.get(connectionId)?.status ?? 'disconnected';
  }

  async logout(connectionId: string): Promise<void> {
    const session = this.sessions.get(connectionId);
    if (session) {
      this.stopHeartbeat(session);
    }
    await this.provider.logout(connectionId);
    this.sessions.delete(connectionId);
    await this.redis.del(
      whatsappStatusKey(connectionId),
      whatsappQrKey(connectionId),
    );
  }

  sendText(
    connectionId: string,
    chatId: string,
    text: string,
  ): Promise<SendResultInterface> {
    return this.provider.sendText(connectionId, chatId, text);
  }

  sendImages(
    connectionId: string,
    groupId: string,
    imageUrls: string[],
    caption?: string,
  ): Promise<SendResultInterface> {
    return this.provider.sendImages(connectionId, groupId, imageUrls, caption);
  }

  sendMedia(
    connectionId: string,
    chatId: string,
    media: WhatsappMediaPayload,
    options?: { caption?: string; sendAudioAsVoice?: boolean },
  ): Promise<SendResultInterface> {
    return this.provider.sendMedia(connectionId, chatId, media, options);
  }

  getMedia(
    connectionId: string,
    messageId: string,
  ): Promise<WhatsappMediaPayload | null> {
    return this.provider.getMedia(connectionId, messageId);
  }

  getContact(connectionId: string, chatId: string): Promise<WhatsappContact> {
    return this.provider.getContact(connectionId, chatId);
  }

  getAllChats(connectionId: string): Promise<WhatsappChatSummary[]> {
    return this.provider.getAllChats(connectionId);
  }

  getChatMessages(
    connectionId: string,
    chatId: string,
    limit = 50,
  ): Promise<WhatsappRawMessage[]> {
    return this.provider.getChatMessages(connectionId, chatId, limit);
  }

  deleteChat(
    connectionId: string,
    chatId: string,
  ): Promise<{ ok: boolean; error?: string }> {
    return this.provider.deleteChat(connectionId, chatId);
  }

  // ── WhatsappBaileysEventListener ───────────────────────────────────
  // Reemplaza a los client.on('qr'|'ready'|'disconnected'|'auth_failure'|
  // 'call'|'message', ...) que antes vivían en initClientForConnection().

  async onQr(connectionId: string, qrRaw: string): Promise<void> {
    this.logger.log(`[${connectionId}] QR generado`);
    const session = this.sessions.get(connectionId);
    if (session) session.status = 'waiting_qr';

    try {
      const qrPngBuffer = await QRCode.toBuffer(qrRaw, {
        type: 'png',
        scale: 8,
      });
      const qrDataUrl = await QRCode.toDataURL(qrRaw);

      // Baileys renueva el QR cada ~20 s, así que un TTL corto basta y evita
      // que un 'waiting_qr' huérfano (runtime caído) dure 10 minutos.
      const cacheValue: WhatsappQrCacheValue = {
        qrPngBase64: qrPngBuffer.toString('base64'),
        generatedAt: Date.now(),
      };
      // Primero el QR y DESPUÉS el estado: así, si la API ve 'waiting_qr'
      // el QR ya está disponible.
      await this.redis.set(
        whatsappQrKey(connectionId),
        JSON.stringify(cacheValue),
        'EX',
        QR_TTL_SECONDS,
      );
      await this.redis.set(
        whatsappStatusKey(connectionId),
        'waiting_qr',
        'EX',
        QR_TTL_SECONDS,
      );

      await this.publishLiveEvent({ kind: 'qr', connectionId, qr: qrDataUrl });
    } catch (e) {
      // Antes un fallo acá era un unhandledRejection mudo dentro del handler
      // de Baileys y el estado quedaba a medias.
      this.logger.error(
        `[${connectionId}] no se pudo publicar el QR: ${e.message}`,
      );
    }
  }

  async onStatus(
    connectionId: string,
    status: WhatsappConnectionStatus,
  ): Promise<void> {
    const session = this.sessions.get(connectionId) ?? { status };
    session.status = status;
    this.sessions.set(connectionId, session);

    if (status === 'connected') {
      // TTL en vez de guardar para siempre: si este proceso muere o el
      // socket se cae sin pasar por 'disconnected', la clave expira sola
      // en vez de dejar a la API/frontend creyendo que sigue conectado. El
      // heartbeat de abajo la va renovando mientras de verdad lo esté.
      await this.redis.set(
        whatsappStatusKey(connectionId),
        'connected',
        'EX',
        STATUS_HEARTBEAT_TTL_SECONDS,
      );
      await this.redis.del(whatsappQrKey(connectionId));
      this.startHeartbeat(connectionId, session);
    } else {
      this.stopHeartbeat(session);
      await this.redis.set(whatsappStatusKey(connectionId), status);
      if (status === 'disconnected' || status === 'auth_failed') {
        await this.redis.del(whatsappQrKey(connectionId));
      }
    }

    await this.publishLiveEvent({ kind: 'status', connectionId, status });
  }

  async onCall(
    connectionId: string,
    event: Omit<WhatsappCallEvent, 'kind' | 'connectionId'>,
  ): Promise<void> {
    await this.publishLiveEvent({ kind: 'call', connectionId, ...event });
  }

  async onMessagePersist(
    connectionId: string,
    payload: WhatsappMessagePersistPayload,
  ): Promise<void> {
    await this.enqueuePersistEvent(payload);
  }

  async onMessageReceivedLive(
    connectionId: string,
    event: Omit<WhatsappMessageReceivedEvent, 'kind' | 'connectionId'>,
  ): Promise<void> {
    await this.publishLiveEvent({
      kind: 'message-received',
      connectionId,
      ...event,
    });
  }

  // ── privado (idéntico a la versión con whatsapp-web.js) ─────────────

  private async enqueuePersistEvent(payload: WhatsappMessagePersistPayload) {
    const job: WhatsappPersistEventJob = { payload };
    await this.persistQueue.add(WHATSAPP_PERSIST_EVENT_JOB_NAME, job, {
      attempts: 4,
      backoff: { type: 'exponential', delay: 5000 },
      removeOnComplete: true,
      removeOnFail: 100,
    });
  }

  private async publishLiveEvent(event: WhatsappLiveEvent) {
    await this.redis.publish(
      WHATSAPP_LIVE_EVENTS_CHANNEL,
      JSON.stringify(event),
    );
  }

  /**
   * Al arrancar el proceso, `this.sessions` está vacío por definición —
   * recién arranca, no hay ningún socket vivo todavía. Cualquier status
   * "activo" (`connected`/`connecting`/`waiting_qr`) que siga en Redis es
   * entonces un remanente de una corrida anterior (crash, redeploy, etc.):
   * lo reseteamos a 'disconnected' para que la API/frontend dejen de
   * mostrar una conexión fantasma y vuelvan a intentar `connect()`.
   */
  private async reconcileStaleStatuses(): Promise<void> {
    let cursor = '0';
    let resetCount = 0;

    try {
      do {
        const [nextCursor, keys] = await this.redis.scan(
          cursor,
          'MATCH',
          'wa:status:*',
          'COUNT',
          100,
        );
        cursor = nextCursor;

        for (const key of keys) {
          const value = (await this.redis.get(
            key,
          )) as WhatsappConnectionStatus | null;
          if (!value || !STALE_STATUSES_ON_BOOT.includes(value)) continue;

          const connectionId = key.slice('wa:status:'.length);
          await this.redis.set(whatsappStatusKey(connectionId), 'disconnected');
          await this.redis.del(whatsappQrKey(connectionId));
          await this.publishLiveEvent({
            kind: 'status',
            connectionId,
            status: 'disconnected',
          });
          resetCount++;
        }
      } while (cursor !== '0');
    } catch (e) {
      this.logger.error(
        `No se pudo reconciliar estados huérfanos en Redis: ${e.message}`,
      );
      return;
    }

    if (resetCount > 0) {
      this.logger.warn(
        `Se resetearon ${resetCount} estado(s) huérfano(s) en Redis (quedaban de una corrida anterior del runtime).`,
      );
    }
  }

  /**
   * Mientras el socket siga realmente conectado (`provider.isConnected`),
   * renueva el TTL de 'connected' en Redis cada
   * STATUS_HEARTBEAT_INTERVAL_MS. Si el socket se cae sin que el provider
   * llegue a avisar 'disconnected', o el propio chequeo falla, se deja de
   * renovar y la clave expira sola en vez de mentir indefinidamente.
   */
  private startHeartbeat(connectionId: string, session: RuntimeSession): void {
    this.stopHeartbeat(session);
    session.heartbeatInterval = setInterval(async () => {
      if (session.status !== 'connected') {
        this.stopHeartbeat(session);
        return;
      }
      if (this.provider.isConnected(connectionId)) {
        await this.redis.set(
          whatsappStatusKey(connectionId),
          'connected',
          'EX',
          STATUS_HEARTBEAT_TTL_SECONDS,
        );
      } else {
        this.logger.warn(
          `[${connectionId}] heartbeat detectó el socket caído; se deja de renovar el TTL.`,
        );
        this.stopHeartbeat(session);
      }
    }, STATUS_HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeat(session: RuntimeSession): void {
    if (session.heartbeatInterval) {
      clearInterval(session.heartbeatInterval);
      session.heartbeatInterval = undefined;
    }
  }
}
