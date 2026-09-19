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
import { Client, RemoteAuth, Message, MessageMedia } from 'whatsapp-web.js';
import {
  SendResultInterface,
  WhatsappChatSummary,
  WhatsappContact,
  WhatsappConnectionStatus,
  WhatsappMediaPayload,
  WhatsappMessagePersistPayload,
  WhatsappMessageType,
  WhatsappRawMessage,
  WhatsappLiveEvent,
  WhatsappPersistEventJob,
  WhatsappQrCacheValue,
  WHATSAPP_PERSIST_EVENTS_QUEUE,
  WHATSAPP_PERSIST_EVENT_JOB_NAME,
  WHATSAPP_LIVE_EVENTS_CHANNEL,
  whatsappStatusKey,
  whatsappQrKey,
} from 'shared/whatsapp-contracts';
import { redisOptions } from 'src/config/bullmq.config';
import { WhatsappRemoteAuthStore } from './whatsapp-remote-auth.store';

interface RuntimeSession {
  client: Client | null;
  status: WhatsappConnectionStatus;
  /** Timer del heartbeat que refresca `wa:status:*` mientras siga 'connected'. */
  heartbeatInterval?: NodeJS.Timeout;
}

/**
 * Estados que, si se encuentran en Redis al arrancar el proceso, son por
 * definición huérfanos: recién arranca, así que `this.sessions` está vacío
 * y no puede haber ningún Client vivo detrás de ellos todavía.
 */
const STALE_STATUSES_ON_BOOT: WhatsappConnectionStatus[] = [
  'connected',
  'connecting',
  'waiting_qr',
];

/**
 * TTL de la clave 'connected' en Redis y frecuencia con la que el heartbeat
 * la refresca. Si el proceso muere o el Client se cae sin disparar el
 * evento 'disconnected' (ej. Chrome matado por OOM con headless:false), la
 * clave expira sola en vez de quedar diciendo 'connected' para siempre.
 */
const STATUS_HEARTBEAT_TTL_SECONDS = 60;
const STATUS_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * Es `whatsapp-web.provider.ts` movido aquí, con 3 cambios de fondo:
 *
 *  1. `LocalAuth` → `RemoteAuth` (vía WhatsappRemoteAuthStore) — la sesión
 *     ya no depende del disco de este container puntual.
 *  2. Cada `eventEmitter.emit(...)` de antes se reparte en dos caminos:
 *       - eventos "en vivo" (qr/status/call/message-received) → Redis Pub/Sub
 *       - `message.persist` → cola BullMQ (necesita reintentos si falla el guardado)
 *  3. `qr`/`ready`/`disconnected` ahora también escriben el estado actual en
 *     Redis (claves `wa:status:*` / `wa:qr:*`) para que la API lo lea sin
 *     pasar por la cola de comandos.
 *
 * PENDIENTE A PROPÓSITO: `getAllChats` de aquí solo cubre resúmenes de chat
 * (lo que hoy expone `getChats`/`getGroups`). El fetch de mensajes por chat
 * que hoy hace `WhatsappSyncService.syncMessagesForChat` con el `Client` vivo
 * todavía no se movió — lo resolvemos cuando toquemos `whatsapp-sync.service.ts`.
 *
 * Nota de nombres: se estandarizó `sessionId` → `connectionId` en todos los
 * eventos, a diferencia del código original donde `call` y `message.received`
 * usaban `sessionId`.
 *
 * --- Fix de la ronda de revisión: status 'connected' huérfano en Redis ---
 * Antes, 'connected'/'connecting'/'waiting_qr' (salvo waiting_qr) se
 * guardaban en Redis SIN TTL. Si este proceso se reiniciaba, `this.sessions`
 * volvía a estar vacío (sin ningún Client real) pero Redis seguía diciendo
 * 'connected' para siempre — la API lo leía tal cual, el frontend confiaba
 * en eso y nunca volvía a llamar a `connect()`, así que el browser
 * (headless:false) nunca se abría de nuevo. Dos cambios lo resuelven:
 *   1. `reconcileStaleStatuses()` en `onModuleInit()`: al arrancar, resetea
 *      a 'disconnected' cualquier status "activo" que haya quedado en Redis
 *      de una corrida anterior (por definición huérfano, recién arranca).
 *   2. Heartbeat mientras el cliente sigue 'connected': refresca la clave
 *      con TTL corto, verificando `client.getState()` de verdad. Si el
 *      proceso muere o el Client se cae sin disparar 'disconnected', la
 *      clave expira sola en vez de quedar mintiendo indefinidamente.
 */
@Injectable()
export class WhatsappRuntimeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WhatsappRuntimeService.name);
  private readonly sessions = new Map<string, RuntimeSession>();
  private readonly redis: Redis;

  constructor(
    @InjectQueue(WHATSAPP_PERSIST_EVENTS_QUEUE)
    private readonly persistQueue: Queue,
    private readonly authStore: WhatsappRemoteAuthStore,
  ) {
    this.redis = new Redis(redisOptions);
  }

  async onModuleInit() {
    await this.reconcileStaleStatuses();
    this.logger.log(
      'Runtime de WhatsApp listo — las sesiones se crean bajo demanda.',
    );
  }

  async onModuleDestroy() {
    for (const session of this.sessions.values()) {
      this.stopHeartbeat(session);
      try {
        await session.client?.destroy();
      } catch {
        /* noop */
      }
    }
    this.redis.disconnect();
  }

  // ── contrato que espera WhatsappCommandsProcessor ──────────────────

  async connect(connectionId: string): Promise<void> {
    const existing = this.sessions.get(connectionId);
    if (
      existing &&
      ['connecting', 'waiting_qr', 'connected'].includes(existing.status)
    ) {
      return; // idempotente: seguro de reintentar
    }

    const session: RuntimeSession = existing ?? {
      client: null,
      status: 'disconnected',
    };
    this.sessions.set(connectionId, session);
    session.status = 'connecting';
    await this.redis.set(whatsappStatusKey(connectionId), 'connecting');

    await this.initClientForConnection(connectionId, session);
  }

  getStatus(connectionId: string): WhatsappConnectionStatus {
    return this.sessions.get(connectionId)?.status ?? 'disconnected';
  }

  async logout(connectionId: string): Promise<void> {
    const session = this.sessions.get(connectionId);
    if (session) {
      this.stopHeartbeat(session);
    }
    if (session?.client) {
      try {
        await session.client.destroy();
      } catch (e) {
        this.logger.warn(
          `[${connectionId}] error al destruir cliente: ${e.message}`,
        );
      }
    }
    this.sessions.delete(connectionId);
    await this.authStore.delete({ session: connectionId });
    await this.redis.del(
      whatsappStatusKey(connectionId),
      whatsappQrKey(connectionId),
    );
  }

  async sendText(
    connectionId: string,
    chatId: string,
    text: string,
  ): Promise<SendResultInterface> {
    const client = this.getConnectedClient(connectionId);
    if (!client) return { ok: false, error: 'WhatsApp no está conectado' };

    try {
      let targetId = chatId;
      if (chatId.endsWith('@lid')) {
        try {
          const contact = await client.getContactById(chatId);
          if (contact?.id?._serialized) targetId = contact.id._serialized;
        } catch (resolveErr) {
          this.logger.debug(
            `[${connectionId}] no se pudo resolver @lid, se usa el original: ${resolveErr.message}`,
          );
        }
      }
      const msg = await client.sendMessage(targetId, text);
      await this.persistOutgoing(connectionId, msg);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  async sendImages(
    connectionId: string,
    groupId: string,
    imageUrls: string[],
    caption?: string,
  ): Promise<SendResultInterface> {
    const client = this.getConnectedClient(connectionId);
    if (!client) return { ok: false, error: 'WhatsApp no está conectado' };

    const urls = (imageUrls || []).filter((u) => u?.trim());
    if (!urls.length) return { ok: false, error: 'No hay imágenes válidas' };

    try {
      for (const [i, url] of urls.entries()) {
        const media = await this.loadMedia(url);
        const options = i === 0 && caption ? { caption } : {};
        const msg = await client.sendMessage(groupId, media, options);
        await this.persistOutgoing(connectionId, msg);
      }
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  async sendMedia(
    connectionId: string,
    chatId: string,
    media: WhatsappMediaPayload,
    options: { caption?: string; sendAudioAsVoice?: boolean } = {},
  ): Promise<SendResultInterface> {
    const client = this.getConnectedClient(connectionId);
    if (!client) return { ok: false, error: 'WhatsApp no está conectado' };

    try {
      const messageMedia = new MessageMedia(
        media.mimetype,
        media.data,
        media.filename,
      );
      const msg = await client.sendMessage(chatId, messageMedia, {
        caption: options.caption,
        sendAudioAsVoice:
          options.sendAudioAsVoice ?? media.mimetype.startsWith('audio/'),
      });
      await this.persistOutgoing(connectionId, msg);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  async getMedia(
    connectionId: string,
    messageId: string,
  ): Promise<WhatsappMediaPayload | null> {
    const client = this.getConnectedClient(connectionId);
    if (!client) return null;

    try {
      const msg = await client.getMessageById(messageId);
      if (!msg?.hasMedia) return null;
      const media = await msg.downloadMedia();
      if (!media) return null;
      return {
        mimetype: media.mimetype,
        data: media.data,
        filename: media.filename,
      };
    } catch (e) {
      this.logger.warn(
        `[${connectionId}] getMedia falló para ${messageId}: ${e.message}`,
      );
      return null;
    }
  }

  async getContact(
    connectionId: string,
    chatId: string,
  ): Promise<WhatsappContact> {
    const client = this.getConnectedClient(connectionId);
    if (!client) throw new Error('WhatsApp no está conectado');

    const contact = await client.getContactById(chatId);
    return {
      chatId: contact.id._serialized,
      name: contact.pushname || contact.number,
      phoneNumber: contact.number,
    };
  }

  async getAllChats(connectionId: string): Promise<WhatsappChatSummary[]> {
    const client = this.getConnectedClient(connectionId);
    if (!client) throw new Error('WhatsApp no está conectado');

    const state = await client.getState().catch(() => null);
    if (state !== 'CONNECTED') {
      throw new Error(
        `WhatsApp no está listo (estado: ${state ?? 'desconocido'})`,
      );
    }

    try {
      const [rawChats, contacts] = await Promise.all([
        client.getChats(),
        client.getContacts(),
      ]);
      // BUG encontrado: WhatsApp Web incluye en getChats() los canales que
      // sigues (@newsletter), listas de difusión (@broadcast) y estados
      // (@status), NO solo chats 1:1 (@c.us) y grupos (@g.us). Como esos no
      // son personas con las que hablaste, salen sin nombre real → "Usuario
      // desconocido" en el frontend. Esto es casi seguro lo que estás viendo.
      const chats = rawChats.filter((c) => this.isRealChat(c.id._serialized));

      const isSavedContact = new Map<string, boolean>();
      for (const contact of contacts) {
        isSavedContact.set(contact.id._serialized, contact.isMyContact);
      }

      return chats.map((c) => ({
        chatId: c.id._serialized,
        name: c.name || c.id.user,
        isGroup: c.isGroup,
        lastMessage: c.lastMessage?.body,
        lastMessageAt: c.lastMessage?.timestamp,
        unreadCount: c.unreadCount,
        participantsCount: c.isGroup
          ? (c as any).participants?.length
          : undefined,
        isSavedContact: c.isGroup
          ? undefined
          : isSavedContact.get(c.id._serialized),
      }));
    } catch (err) {
      this.logger.error(
        `[${connectionId}] getAllChats falló: ${err?.message || err}`,
      );
      throw new Error(
        'No se pudieron obtener los chats de WhatsApp, intenta de nuevo',
      );
    }
  }

  /**
   * Reemplaza la parte de `WhatsappSyncService.syncMessagesForChat` que
   * tocaba el `Client` directo (`getChatById` + `chat.fetchMessages`). El
   * guardado en Postgres se queda en la API — aquí solo se trae la data.
   */
  async getChatMessages(
    connectionId: string,
    chatId: string,
    limit = 50,
  ): Promise<WhatsappRawMessage[]> {
    const client = this.getConnectedClient(connectionId);
    if (!client) throw new Error('WhatsApp no está conectado');

    const chat = await client.getChatById(chatId);
    const messages = await chat.fetchMessages({ limit });
    const meId = client.info?.wid?._serialized;

    return messages.map((m) => ({
      messageId: m.id.id,
      fromMe: m.fromMe,
      body: m.body,
      timestamp: m.timestamp,
      ack: m.ack,
      isGroup: chat.isGroup,
      type: this.mapMessageType(m.type),
      hasMedia: m.hasMedia,
      author: chat.isGroup ? m.author : undefined,
      serializedId: m.id._serialized,
      mentionsMe: !!meId && (m.mentionedIds ?? []).includes(meId),
    }));
  }

  /**
   * Elimina el chat del WhatsApp real — equivalente a "Eliminar chat" en la
   * app oficial: borra la conversación de tu vista, no envía nada al otro
   * lado ni afecta lo que la otra persona ve.
   */
  async deleteChat(
    connectionId: string,
    chatId: string,
  ): Promise<{ ok: boolean; error?: string }> {
    const client = this.getConnectedClient(connectionId);
    if (!client) return { ok: false, error: 'WhatsApp no está conectado' };

    try {
      const chat = await client.getChatById(chatId);
      await chat.delete();
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  // ── privado ─────────────────────────────────────────────────────

  private getConnectedClient(connectionId: string): Client | null {
    const session = this.sessions.get(connectionId);
    return session?.status === 'connected' ? session.client : null;
  }

  private async loadMedia(url: string) {
    const isLocal = !url.startsWith('http://') && !url.startsWith('https://');
    return isLocal
      ? MessageMedia.fromFilePath(url)
      : MessageMedia.fromUrl(url, { unsafeMime: true });
  }

  /**
   * true solo para chats 1:1 (@c.us) y grupos (@g.us). Excluye canales
   * (@newsletter), listas de difusión (@broadcast) y estados (@status) —
   * WhatsApp los mezcla en getChats()/el evento 'message' pero no son
   * conversaciones con personas reales.
   */
  private isRealChat(chatIdSerialized: string): boolean {
    return (
      chatIdSerialized.endsWith('@c.us') || chatIdSerialized.endsWith('@g.us')
    );
  }

  private mapMessageType(waType: string): WhatsappMessageType {
    const known = [
      'chat',
      'image',
      'video',
      'audio',
      'ptt',
      'document',
      'sticker',
      'call_log',
      'location',
      'vcard',
    ];
    return known.includes(waType) ? (waType as WhatsappMessageType) : 'unknown';
  }

  private async persistOutgoing(connectionId: string, msg: Message) {
    try {
      const chat = await msg.getChat();
      await this.enqueuePersistEvent({
        connectionId,
        chatId: chat.id._serialized,
        chatName: chat.name || chat.id.user,
        messageId: msg.id.id,
        serializedId: msg.id._serialized,
        fromMe: true,
        body: msg.body,
        timestamp: msg.timestamp,
        ack: msg.ack,
        unreadCount: chat.unreadCount,
        isGroup: chat.isGroup,
        type: this.mapMessageType(msg.type),
        hasMedia: msg.hasMedia,
        mentionsMe: false,
      });
    } catch (e) {
      this.logger.warn(
        `[${connectionId}] no se pudo encolar el mensaje saliente para persistir: ${e.message}`,
      );
    }
  }

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
   * recién arranca, no hay ningún Client vivo todavía. Cualquier status
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

          // El key tiene el formato de whatsappStatusKey(connectionId).
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
   * Mientras el cliente siga realmente conectado (`client.getState()`),
   * renueva el TTL de 'connected' en Redis cada
   * STATUS_HEARTBEAT_INTERVAL_MS. Si el Client se cae sin disparar
   * 'disconnected', o el propio chequeo falla, se deja de renovar y la
   * clave expira sola en vez de mentir indefinidamente.
   */
  private startHeartbeat(
    connectionId: string,
    session: RuntimeSession,
    client: Client,
  ): void {
    this.stopHeartbeat(session);
    session.heartbeatInterval = setInterval(async () => {
      if (session.status !== 'connected') {
        this.stopHeartbeat(session);
        return;
      }
      try {
        const state = await client.getState();
        if (state === 'CONNECTED') {
          await this.redis.set(
            whatsappStatusKey(connectionId),
            'connected',
            'EX',
            STATUS_HEARTBEAT_TTL_SECONDS,
          );
        } else {
          this.logger.warn(
            `[${connectionId}] heartbeat detectó estado inconsistente (${state}); se deja de renovar el TTL.`,
          );
          this.stopHeartbeat(session);
        }
      } catch (e) {
        this.logger.warn(
          `[${connectionId}] heartbeat falló (${e.message}); se deja de renovar el TTL.`,
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

  private async initClientForConnection(
    connectionId: string,
    session: RuntimeSession,
  ): Promise<void> {
    const client = new Client({
      authStrategy: new RemoteAuth({
        clientId: connectionId,
        store: this.authStore,
        // Mínimo permitido por RemoteAuth es 60000ms — 5 min es razonable
        // para no golpear Postgres en cada mensaje.
        backupSyncIntervalMs: 5 * 60 * 1000,
      }),
      puppeteer: {
        // Decisión de Fase 0: headless 'new' en vez del headless:false
        // original. En un servidor Linux sin pantalla, headless:false
        // requeriría Xvfb — lo dejamos pendiente de validar en el piloto
        // de la Fase 2 (puede afectar la tasa de detección/baneo).
        headless: false,
        protocolTimeout: 300000,
        args: [
          '--disable-dev-shm-usage',
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--no-first-run',
          '--no-zygote',
          '--disable-gpu',
        ],
      },
    });
    session.client = client;

    client.on('qr', async (qr) => {
      this.logger.log(`[${connectionId}] QR generado`);
      session.status = 'waiting_qr';

      const qrPngBuffer = await QRCode.toBuffer(qr, { type: 'png', scale: 8 });
      const qrDataUrl = await QRCode.toDataURL(qr);

      await this.redis.set(
        whatsappStatusKey(connectionId),
        'waiting_qr',
        'EX',
        600,
      );
      const cacheValue: WhatsappQrCacheValue = {
        qrPngBase64: qrPngBuffer.toString('base64'),
        generatedAt: Date.now(),
      };
      await this.redis.set(
        whatsappQrKey(connectionId),
        JSON.stringify(cacheValue),
        'EX',
        600,
      );

      await this.publishLiveEvent({ kind: 'qr', connectionId, qr: qrDataUrl });
    });

    client.on('authenticated', () => {
      this.logger.log(`[${connectionId}] autenticado`);
    });

    client.on('ready', async () => {
      session.status = 'connected';
      // TTL en vez de guardar para siempre: si este proceso muere o el
      // Client se cae sin pasar por 'disconnected', la clave expira sola
      // en vez de dejar a la API/frontend creyendo que sigue conectado.
      // El heartbeat de abajo la va renovando mientras de verdad lo esté.
      await this.redis.set(
        whatsappStatusKey(connectionId),
        'connected',
        'EX',
        STATUS_HEARTBEAT_TTL_SECONDS,
      );
      await this.redis.del(whatsappQrKey(connectionId));
      await this.publishLiveEvent({
        kind: 'status',
        connectionId,
        status: 'connected',
      });
      this.startHeartbeat(connectionId, session, client);
    });

    client.on('disconnected', async (reason) => {
      this.logger.warn(`[${connectionId}] desconectado: ${reason}`);
      this.stopHeartbeat(session);
      session.status = 'disconnected';
      await this.redis.set(whatsappStatusKey(connectionId), 'disconnected');
      await this.publishLiveEvent({
        kind: 'status',
        connectionId,
        status: 'disconnected',
      });
    });

    client.on('auth_failure', async (msg) => {
      this.logger.error(`[${connectionId}] fallo de autenticación: ${msg}`);
      this.stopHeartbeat(session);
      session.status = 'auth_failed';
      await this.redis.set(whatsappStatusKey(connectionId), 'auth_failed');
      await this.publishLiveEvent({
        kind: 'status',
        connectionId,
        status: 'auth_failed',
      });
    });

    client.on('call', async (call) => {
      await this.publishLiveEvent({
        kind: 'call',
        connectionId,
        from: call.from,
        isVideo: call.isVideo,
        isGroup: call.isGroup,
        timestamp: call.timestamp,
      });
    });

    client.on('message', async (msg) => {
      try {
        const chat = await msg.getChat();
        if (!this.isRealChat(chat.id._serialized)) {
          return; // canal/newsletter/broadcast/status — no es una persona real
        }
        const isGroup = chat.isGroup;

        let author: string | undefined;
        let authorName: string | undefined;
        if (isGroup && !msg.fromMe) {
          const contact = await msg.getContact();
          author = contact.id._serialized;
          authorName = contact.pushname || contact.number;
        }

        const meId = client.info?.wid?._serialized;
        const myNumber = client.info?.wid?.user;
        let mentionsMe = false;
        for (const id of msg.mentionedIds ?? []) {
          if (id === meId) {
            mentionsMe = true;
            break;
          }
          if (id.endsWith('@lid')) {
            try {
              const contact = await client.getContactById(id);
              if (contact?.number === myNumber) {
                mentionsMe = true;
                break;
              }
            } catch {
              /* no se pudo resolver el @lid, seguimos sin marcar mención */
            }
          }
        }

        await this.enqueuePersistEvent({
          connectionId,
          chatId: chat.id._serialized,
          chatName: chat.name || chat.id.user,
          messageId: msg.id.id,
          serializedId: msg.id._serialized,
          fromMe: msg.fromMe,
          body: msg.body,
          timestamp: msg.timestamp,
          ack: msg.ack,
          unreadCount: chat.unreadCount,
          isGroup,
          type: this.mapMessageType(msg.type),
          hasMedia: msg.hasMedia,
          author,
          authorName,
          mentionsMe,
        });

        if (msg.fromMe) return;

        const contact = await msg.getContact();
        await this.publishLiveEvent({
          kind: 'message-received',
          connectionId,
          chatId: msg.from,
          isGroup,
          contact: {
            chatId: contact.id._serialized,
            name: contact.pushname || contact.number,
            phoneNumber: contact.number,
          },
          text: msg.body,
          messageId: msg.id.id,
          serializedId: msg.id._serialized,
          type: this.mapMessageType(msg.type),
          hasMedia: msg.hasMedia,
        });
      } catch (e) {
        this.logger.error(
          `[${connectionId}] error procesando mensaje entrante: ${e.message}`,
        );
      }
    });

    try {
      await client.initialize();
    } catch (e) {
      this.stopHeartbeat(session);
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
}
