import { Injectable, Logger } from '@nestjs/common';
import makeWASocket, {
  Browsers,
  DisconnectReason,
  downloadMediaMessage,
  isJidBroadcast,
  isJidNewsletter,
  isLidUser,
  isPnUser,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  proto,
  WAMessageKey,
  WASocket,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import * as fs from 'fs/promises';
import pino from 'pino';
import {
  SendResultInterface,
  WhatsappCallEvent,
  WhatsappChatSummary,
  WhatsappContact,
  WhatsappConnectionStatus,
  WhatsappMediaPayload,
  WhatsappMessagePersistPayload,
  WhatsappMessageReceivedEvent,
  WhatsappMessageType,
  WhatsappRawMessage,
} from '../../shared/whatsapp-contracts';
import { WhatsappBaileysAuthStore } from './whatsapp-baileys-auth.store';

/**
 * Lo que `WhatsappRuntimeService` tiene que implementar para recibir los
 * eventos del provider — es el reemplazo de los `client.on('qr', ...)`,
 * `client.on('ready', ...)`, etc. que antes se escuchaban directo sobre el
 * `Client` de whatsapp-web.js. El provider no sabe nada de Redis ni de
 * BullMQ: solo traduce eventos crudos de Baileys a estos callbacks.
 */
export interface WhatsappBaileysEventListener {
  onQr(connectionId: string, qrRaw: string): void | Promise<void>;
  onStatus(
    connectionId: string,
    status: WhatsappConnectionStatus,
  ): void | Promise<void>;
  onCall(
    connectionId: string,
    event: Omit<WhatsappCallEvent, 'kind' | 'connectionId'>,
  ): void | Promise<void>;
  onMessagePersist(
    connectionId: string,
    payload: WhatsappMessagePersistPayload,
  ): void | Promise<void>;
  onMessageReceivedLive(
    connectionId: string,
    event: Omit<WhatsappMessageReceivedEvent, 'kind' | 'connectionId'>,
  ): void | Promise<void>;
}

interface InternalChatEntry extends WhatsappChatSummary {
  lastMessageKey?: proto.IMessageKey;
  lastMessageTimestamp?: number;
}

interface BaileysConnectionSession {
  sock: WASocket;
  listener: WhatsappBaileysEventListener;
  loggingOut: boolean;
  chats: Map<string, InternalChatEntry>;
  contacts: Map<string, WhatsappContact>;
  messagesByChat: Map<string, WhatsappRawMessage[]>;
  rawMessagesById: Map<string, proto.IWebMessageInfo>;
  groupMetadataCache: Map<string, any>;
  /** ids ya procesados: evita duplicar el eco 'append' de un mensaje que enviamos nosotros. */
  seenMessageIds: Set<string>;
  /** grupo -> timestamp hasta el cual NO se vuelve a pedir su metadata (forbidden / rate-overlimit). */
  groupMetaBlockedUntil: Map<string, number>;
  lastGroupsFetch: number;
  groupsFetchInFlight?: Promise<void>;
}

// Límites del estado en memoria por conexión — Baileys, a diferencia de
// whatsapp-web.js, "no mantiene un estado interno de chats/contactos/
// mensajes" (textual de su propia doc): hay que armarlo uno mismo a partir
// de los eventos. Se cachea acotado en memoria en vez de todo el historial;
// ver la nota en getChatMessages() sobre esta limitación.
const MAX_MESSAGES_PER_CHAT = 100;
const MAX_CACHED_RAW_MESSAGES = 500;
const MAX_SEEN_MESSAGE_IDS = 5000;

// Metadata de grupos: UNA sola petición (groupFetchAllParticipating) para todos
// los grupos, refrescada como máximo cada 10 min. Las peticiones por grupo que
// fallan se bloquean un tiempo: 'forbidden' (ya no estás en el grupo) casi
// nunca se arregla solo; 'rate-overlimit' necesita que WhatsApp se enfríe.
const GROUPS_REFRESH_MS = 10 * 60_000;
const GROUP_FORBIDDEN_BLOCK_MS = 6 * 60 * 60_000;
const GROUP_RATE_LIMIT_BLOCK_MS = 10 * 60_000;

const LEGACY_INDIVIDUAL_SUFFIX = '@c.us';
const BAILEYS_INDIVIDUAL_SUFFIX = '@s.whatsapp.net';

/**
 * Reemplaza la parte de `whatsapp-runtime.service.ts` que antes tocaba
 * directo el `Client` de whatsapp-web.js. Encapsula TODO el detalle de
 * Baileys: creación del socket, mapeo de eventos crudos a los tipos de
 * `shared/whatsapp-contracts`, envío/descarga de media, y una decisión de
 * diseño importante:
 *
 * --- Normalización de JIDs ---
 * Baileys identifica los chats 1:1 como `<numero>@s.whatsapp.net`;
 * whatsapp-web.js (y toda la base de datos/frontend actuales) usa
 * `<numero>@c.us`. Los grupos coinciden en ambos (`@g.us`), igual que
 * `status@broadcast` y `@newsletter`. En vez de migrar el chatId en toda la
 * API/BD/frontend, este provider normaliza en el borde: todo lo que sale
 * hacia `WhatsappBaileysEventListener` / los métodos públicos usa el
 * formato legacy (`@c.us`), y solo se traduce a `@s.whatsapp.net` justo
 * antes de llamar a un método del socket. Así ningún otro archivo del
 * sistema necesitó tocarse (confirmado: `@c.us` solo aparece en un
 * comentario de `shared/whatsapp-contracts/types.ts`, y `@g.us` ya se
 * arma igual en ambos proveedores).
 *
 * --- Reconexión ---
 * A diferencia del `Client` de whatsapp-web.js (que reconectaba solo), un
 * socket de Baileys es "desechable" según su propia documentación: si la
 * conexión se cierra por un motivo que no es un logout real, hay que crear
 * un socket nuevo desde cero. Eso lo hace `handleConnectionUpdate` acá
 * abajo, con un backoff simple.
 *
 * --- Fixes de la ronda de verificación contra docs.baileys.wiki ---
 * Revisando la doc oficial (Session management + Troubleshooting) contra la
 * primera versión de este archivo:
 *  1. `keys` del auth state ahora se envuelve con `makeCacheableSignalKeyStore`
 *     — sin esto, cada mensaje dispara lookups de Signal contra Postgres.
 *  2. Se agregó `getMessage` (backed por `rawMessagesById`) y
 *     `msgRetryCounterCache` — sin `getMessage`, WhatsApp no puede pedirle a
 *     Baileys que reintente un mensaje que falló en destino (el famoso "this
 *     message can take a while" que se cuelga para siempre).
 *  3. Se agregó `cachedGroupMetadata` en la config del socket (antes existía
 *     `getGroupMetadataCached` pero solo se usaba desde `getAllChats`, nunca
 *     se lo pasábamos al propio socket) — sin esto, Baileys pide la metadata
 *     del grupo a WhatsApp en cada mensaje enviado a un grupo, lo cual es
 *     lento y puede gatillar rate limiting.
 *  4. `deleteChat` ya NO llama a `chatModify` cuando no hay un último
 *     mensaje verificado en caché. Antes mandaba `lastMessages: []` como
 *     fallback — la doc de troubleshooting marca ESE MISMO patrón,
 *     literalmente, como causa de desloguear todos los dispositivos
 *     vinculados a la cuenta (no solo esta conexión). Se prefiere fallar el
 *     borrado a arriesgar la sesión completa.
 *  5. `getMedia` usaba `reuploadRequestIfNeeded` (nombre inventado); el
 *     parámetro real de `downloadMediaMessage` es `reuploadRequest`.
 */
@Injectable()
export class WhatsappBaileysProvider {
  private readonly logger = new Logger(WhatsappBaileysProvider.name);
  // BAILEYS_LOG_LEVEL=debug para diagnosticar (ver por qué no llega el QR,
  // códigos de cierre, etc.). 'silent' oculta todo, incluidos los errores.
  private readonly baileysLogger = pino({
    level: process.env.BAILEYS_LOG_LEVEL || 'warn',
  });
  private readonly sessions = new Map<string, BaileysConnectionSession>();
  /** connect() en curso: evita crear dos sockets si llegan dos comandos a la vez. */
  private readonly connecting = new Map<string, Promise<void>>();

  constructor(private readonly authStore: WhatsappBaileysAuthStore) {}

  // ── ciclo de vida ────────────────────────────────────────────────

  connect(
    connectionId: string,
    listener: WhatsappBaileysEventListener,
  ): Promise<void> {
    if (this.sessions.has(connectionId)) return Promise.resolve(); // idempotente
    const inflight = this.connecting.get(connectionId);
    if (inflight) return inflight;
    const p = this.doConnect(connectionId, listener).finally(() =>
      this.connecting.delete(connectionId),
    );
    this.connecting.set(connectionId, p);
    return p;
  }

  private async doConnect(
    connectionId: string,
    listener: WhatsappBaileysEventListener,
  ): Promise<void> {
    const { state, saveCreds } =
      await this.authStore.getAuthState(connectionId);

    // Estas dos estructuras se arman ANTES del socket porque
    // `getMessage`/`cachedGroupMetadata` (parte de la config del socket) las
    // necesitan por closure — luego se reusan tal cual como las cachés de
    // la sesión (`session.rawMessagesById`/`session.groupMetadataCache`).
    const rawMessagesById = new Map<string, proto.IWebMessageInfo>();
    const groupMetadataCache = new Map<string, any>();
    const msgRetryCounterCache = this.createMemoryCacheStore();

    const sock = makeWASocket({
      auth: {
        creds: state.creds,
        // docs.baileys.wiki (Session management): en producción, cada
        // mensaje dispara lookups de claves de Signal — sin esta caché de
        // 5 min en memoria, cada uno de esos lookups pega directo contra
        // Postgres.
        keys: makeCacheableSignalKeyStore(state.keys, this.baileysLogger),
      },
      logger: this.baileysLogger as any,
      browser: Browsers.ubuntu('Chrome'),
      syncFullHistory: false,
      // Con true (default), el celular deja de recibir notificaciones push
      // mientras el runtime esté conectado.
      markOnlineOnConnect: false,
      // Sin esto Baileys procesa/descifra broadcasts y newsletters que igual
      // se descartan más abajo.
      shouldIgnoreJid: (jid: string) =>
        isJidBroadcast(jid) || isJidNewsletter(jid),
      generateHighQualityLinkPreview: false,
      // docs.baileys.wiki (Troubleshooting → "this message can take a
      // while"): sin `getMessage`, WhatsApp no puede pedirle a Baileys que
      // reintente un mensaje que falló en destino — se queda colgado con
      // ese aviso. Se resuelve contra la misma caché que usa getMedia().
      getMessage: async (key) => {
        const raw = key.id ? rawMessagesById.get(key.id) : undefined;
        return raw?.message ?? undefined;
      },
      msgRetryCounterCache: msgRetryCounterCache as any,
      enableAutoSessionRecreation: true,
      enableRecentMessageCache: true,
      // docs.baileys.wiki (Troubleshooting → "Group messages failing or
      // being slow"): sin esto, Baileys pide la metadata del grupo a
      // WhatsApp en CADA mensaje que se manda a un grupo — lento y puede
      // gatillar rate limiting. Se mantiene "caliente" en
      // group-participants.update/groups.update (ver wireEvents).
      cachedGroupMetadata: async (jid) => groupMetadataCache.get(jid),
    });

    const session: BaileysConnectionSession = {
      sock,
      listener,
      loggingOut: false,
      chats: new Map(),
      contacts: new Map(),
      messagesByChat: new Map(),
      rawMessagesById,
      groupMetadataCache,
      seenMessageIds: new Set(),
      groupMetaBlockedUntil: new Map(),
      lastGroupsFetch: 0,
    };
    this.sessions.set(connectionId, session);

    sock.ev.on('creds.update', saveCreds);
    this.wireEvents(connectionId, session);
  }

  async logout(connectionId: string): Promise<void> {
    const session = this.sessions.get(connectionId);
    if (!session) {
      await this.authStore.delete(connectionId);
      return;
    }
    session.loggingOut = true; // evita que handleConnectionClose intente reconectar
    try {
      await session.sock.logout();
    } catch (e) {
      this.logger.warn(`[${connectionId}] error al desloguear: ${e.message}`);
    }
    this.sessions.delete(connectionId);
    await this.authStore.delete(connectionId);
  }

  /** Cierra todos los sockets sin borrar credenciales — para OnModuleDestroy (SIGTERM/reinicio). */
  async destroyAll(): Promise<void> {
    for (const [connectionId, session] of this.sessions.entries()) {
      try {
        session.sock.end(new Error('runtime apagándose'));
      } catch {
        /* noop */
      }
      this.sessions.delete(connectionId);
    }
  }

  isConnected(connectionId: string): boolean {
    return !!this.getConnectedSession(connectionId);
  }

  /** ¿Hay un socket creado (aunque aún no autenticado/esperando QR)? */
  hasSession(connectionId: string): boolean {
    return this.sessions.has(connectionId) || this.connecting.has(connectionId);
  }

  // ── comandos ─────────────────────────────────────────────────────

  async sendText(
    connectionId: string,
    chatId: string,
    text: string,
  ): Promise<SendResultInterface> {
    const session = this.getConnectedSession(connectionId);
    if (!session) return { ok: false, error: 'WhatsApp no está conectado' };

    try {
      const targetJid = await this.resolveSendTarget(session, chatId);
      const sent = await session.sock.sendMessage(targetJid, { text });
      await this.persistOutgoing(connectionId, session, sent);
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
    const session = this.getConnectedSession(connectionId);
    if (!session) return { ok: false, error: 'WhatsApp no está conectado' };

    const urls = (imageUrls || []).filter((u) => u?.trim());
    if (!urls.length) return { ok: false, error: 'No hay imágenes válidas' };

    const targetJid = this.toBaileysJid(groupId);
    try {
      for (const [i, url] of urls.entries()) {
        const imageContent = await this.loadImageContent(url);
        const sent = await session.sock.sendMessage(targetJid, {
          ...imageContent,
          caption: i === 0 ? caption : undefined,
        });
        await this.persistOutgoing(connectionId, session, sent);
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
    const session = this.getConnectedSession(connectionId);
    if (!session) return { ok: false, error: 'WhatsApp no está conectado' };

    try {
      const targetJid = await this.resolveSendTarget(session, chatId);
      const buffer = Buffer.from(media.data, 'base64');
      const isAudio = media.mimetype.startsWith('audio/');
      const asVoice = options.sendAudioAsVoice ?? isAudio;

      const content: any = isAudio
        ? { audio: buffer, mimetype: media.mimetype, ptt: asVoice }
        : media.mimetype.startsWith('image/')
          ? {
              image: buffer,
              caption: options.caption,
              mimetype: media.mimetype,
            }
          : media.mimetype.startsWith('video/')
            ? {
                video: buffer,
                caption: options.caption,
                mimetype: media.mimetype,
              }
            : {
                document: buffer,
                mimetype: media.mimetype,
                fileName: media.filename || 'archivo',
                caption: options.caption,
              };

      const sent = await session.sock.sendMessage(targetJid, content);
      await this.persistOutgoing(connectionId, session, sent);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  async getMedia(
    connectionId: string,
    messageId: string,
  ): Promise<WhatsappMediaPayload | null> {
    const session = this.sessions.get(connectionId);
    if (!session) return null;

    const raw = session.rawMessagesById.get(messageId);
    if (!raw?.message) return null;

    try {
      // Patrón documentado por la propia Baileys para descargar media de un
      // mensaje ya recibido. `reuploadRequest` deja que Baileys pida de
      // nuevo el archivo al servidor si el link original ya expiró (los
      // medios expiran del lado de WhatsApp pasado un tiempo — 404).
      const buffer = (await downloadMediaMessage(
        raw as any,
        'buffer',
        {},
        {
          logger: this.baileysLogger as any,
          reuploadRequest: session.sock.updateMediaMessage,
        },
      )) as Buffer;

      const content = this.unwrapContent(raw.message);
      const mimetype =
        this.extractMimetype(content) || 'application/octet-stream';
      return {
        mimetype,
        data: buffer.toString('base64'),
        filename: content?.documentMessage?.fileName || undefined,
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
    const session = this.requireConnectedSession(connectionId);
    const cached = session.contacts.get(chatId);
    if (cached) return cached;

    const baileysJid = this.toBaileysJid(chatId);
    const phoneNumber = this.jidUser(chatId);
    try {
      const [result] = (await session.sock.onWhatsApp(baileysJid)) ?? [];
      if (!result?.exists) {
        this.logger.debug(
          `[${connectionId}] ${chatId} no figura en WhatsApp según onWhatsApp()`,
        );
      }
    } catch (e) {
      this.logger.debug(
        `[${connectionId}] onWhatsApp falló para ${chatId}: ${e.message}`,
      );
    }
    // A diferencia de whatsapp-web.js (perfil completo vía getContactById),
    // Baileys solo confirma existencia — el nombre sale de la caché de
    // contactos si la hay; si no, se usa el número como fallback (mismo
    // criterio que ya usaba whatsapp-web.js: `contact.pushname || contact.number`).
    return { chatId, name: phoneNumber, phoneNumber };
  }

  async getAllChats(connectionId: string): Promise<WhatsappChatSummary[]> {
    const session = this.requireConnectedSession(connectionId);
    const summaries: WhatsappChatSummary[] = [];

    // ANTES: una petición groupMetadata() por cada grupo, en cada sync → los
    // 'rate-overlimit'/'forbidden' del log, y un comando sync-all de minutos
    // que bloqueaba toda la cola de comandos. AHORA: una sola petición
    // (throttled) y después solo se lee de la caché.
    await this.refreshAllGroups(session);

    for (const chat of session.chats.values()) {
      let participantsCount: number | undefined;
      if (chat.isGroup) {
        const metadata = session.groupMetadataCache.get(
          this.toBaileysJid(chat.chatId),
        );
        participantsCount = metadata?.participants?.length;
      }
      summaries.push({
        chatId: chat.chatId,
        name: chat.name,
        isGroup: chat.isGroup,
        lastMessage: chat.lastMessage,
        lastMessageAt: chat.lastMessageAt,
        unreadCount: chat.unreadCount ?? 0,
        participantsCount,
        isSavedContact: chat.isGroup
          ? undefined
          : session.contacts.has(chat.chatId),
      });
    }
    return summaries;
  }

  /**
   * PENDIENTE A PROPÓSITO: a diferencia de `chat.fetchMessages()` de
   * whatsapp-web.js (que pedía historial al propio WhatsApp Web bajo
   * demanda), esto solo devuelve lo que ya pasó por
   * 'messaging-history.set'/'messages.upsert' desde que este socket se
   * conectó (acotado por MAX_MESSAGES_PER_CHAT). Para traer más historial
   * hace falta `sock.fetchMessageHistory(...)` y escuchar los batches
   * adicionales de 'messaging-history.set' que dispara — se deja pendiente
   * igual que otras partes de whatsapp-sync quedaron marcadas como
   * pendientes en la Fase 1.
   */
  async getChatMessages(
    connectionId: string,
    chatId: string,
    limit = 50,
  ): Promise<WhatsappRawMessage[]> {
    this.requireConnectedSession(connectionId);
    const session = this.sessions.get(connectionId)!;
    const cached = session.messagesByChat.get(chatId) ?? [];
    return cached.slice(-limit);
  }

  async deleteChat(
    connectionId: string,
    chatId: string,
  ): Promise<{ ok: boolean; error?: string }> {
    const session = this.getConnectedSession(connectionId);
    if (!session) return { ok: false, error: 'WhatsApp no está conectado' };

    const entry = session.chats.get(chatId);
    const baileysJid = this.toBaileysJid(chatId);

    // docs.baileys.wiki (Troubleshooting → "Getting logged out of all
    // devices"): llamar chatModify con `lastMessages` vacío o incompleto
    // puede gatillar el sistema de seguridad de WhatsApp y desloguear TODOS
    // los dispositivos vinculados a la cuenta — no solo esta conexión. La
    // propia doc lo marca como "dangerous" literalmente con ese ejemplo
    // (lastMessages: []). Antes acá se mandaba ese fallback si no había
    // lastMessageKey en caché; ahora, sin un último mensaje verificado,
    // directamente NO se llama a chatModify — se prefiere fallar el borrado
    // a arriesgar la sesión completa.
    if (!entry?.lastMessageKey) {
      return {
        ok: false,
        error:
          'No hay un último mensaje verificado para este chat todavía — reintentar luego de que llegue o se envíe al menos un mensaje evita un chatModify inseguro (ver docs.baileys.wiki/advanced/troubleshooting).',
      };
    }

    try {
      // Nota de compatibilidad: la forma exacta de `ChatModification` para
      // "eliminar todo el chat" varía entre versiones de Baileys — verificar
      // contra los tipos de la versión instalada si esto tira error.
      await session.sock.chatModify(
        {
          delete: true,
          lastMessages: [
            {
              key: entry.lastMessageKey,
              messageTimestamp: entry.lastMessageTimestamp,
            },
          ],
        } as any,
        baileysJid,
      );
      session.chats.delete(chatId);
      session.messagesByChat.delete(chatId);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  // ── wiring de eventos de Baileys ─────────────────────────────────

  private wireEvents(
    connectionId: string,
    session: BaileysConnectionSession,
  ): void {
    const { sock, listener } = session;

    sock.ev.on('connection.update', async (update: any) => {
      await this.handleConnectionUpdate(connectionId, session, update);
    });

    sock.ev.on(
      'messaging-history.set',
      ({ chats, contacts, messages }: any) => {
        this.handleHistorySet(
          connectionId,
          session,
          chats ?? [],
          contacts ?? [],
          messages ?? [],
        );
      },
    );

    sock.ev.on('chats.upsert', (chats: any[]) =>
      this.upsertChatsFromBaileys(session, chats),
    );
    sock.ev.on('chats.update', (chats: any[]) =>
      this.upsertChatsFromBaileys(session, chats),
    );
    sock.ev.on('contacts.upsert', (contacts: any[]) =>
      this.handleContactsUpsert(session, contacts),
    );
    sock.ev.on('contacts.update', (contacts: any[]) =>
      this.handleContactsUpsert(session, contacts),
    );

    // docs.baileys.wiki (Troubleshooting → "Group messages failing or being
    // slow"): en vez de solo invalidar y esperar a que la próxima
    // getAllChats() la vuelva a pedir, se refresca de inmediato para que
    // cachedGroupMetadata() del socket (usado al ARMAR cada envío a un
    // grupo) nunca encuentre la caché fría.
    sock.ev.on('group-participants.update', async ({ id }: any) => {
      if (id) await this.getGroupMetadataCached(session, id, true);
    });
    // groups.update trae solo los campos que cambiaron: se mezclan con lo que
    // ya hay en caché en vez de pedir el grupo entero a WhatsApp otra vez.
    sock.ev.on('groups.update', (updates: any[]) => {
      for (const u of updates ?? []) {
        if (!u?.id) continue;
        const prev = session.groupMetadataCache.get(u.id);
        if (prev) session.groupMetadataCache.set(u.id, { ...prev, ...u });
      }
    });
    sock.ev.on('groups.upsert', (groups: any[]) => {
      for (const g of groups ?? []) {
        if (g?.id) {
          session.groupMetadataCache.set(g.id, g);
          session.groupMetaBlockedUntil.delete(g.id);
        }
      }
    });

    sock.ev.on('call', (calls: any[]) => {
      for (const call of calls ?? []) {
        listener.onCall(connectionId, {
          from: this.toLegacyId(call.from),
          isVideo: !!call.isVideo,
          isGroup: !!call.isGroup,
          timestamp:
            typeof call.date === 'number'
              ? call.date
              : Math.floor(Date.now() / 1000),
        });
      }
    });

    sock.ev.on(
      'messages.upsert',
      async ({
        messages,
        type,
      }: {
        messages: proto.IWebMessageInfo[];
        type: string;
      }) => {
        for (const msg of messages ?? []) {
          try {
            await this.handleIncomingMessage(connectionId, session, msg, type);
          } catch (e) {
            this.logger.error(
              `[${connectionId}] error procesando mensaje entrante: ${e.message}`,
            );
          }
        }
      },
    );
  }

  private async handleConnectionUpdate(
    connectionId: string,
    session: BaileysConnectionSession,
    update: {
      connection?: string;
      qr?: string;
      lastDisconnect?: { error?: unknown };
    },
  ): Promise<void> {
    const { connection, qr, lastDisconnect } = update;

    // Un socket viejo que cierra tarde no debe pisar la sesión nueva.
    if (connection === 'close' && this.sessions.get(connectionId) !== session) {
      return;
    }

    if (qr) {
      await session.listener.onQr(connectionId, qr);
    }

    if (connection === 'connecting') {
      await session.listener.onStatus(connectionId, 'connecting');
    }

    if (connection === 'open') {
      this.logger.log(`[${connectionId}] conectado`);
      await session.listener.onStatus(connectionId, 'connected');
      // Sin await: no retrasa el 'connected' ni bloquea el handler.
      void this.refreshAllGroups(session);
    }

    if (connection === 'close') {
      const statusCode = (lastDisconnect?.error as Boom | undefined)?.output
        ?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;

      this.sessions.delete(connectionId);

      if (session.loggingOut || loggedOut) {
        this.logger.warn(`[${connectionId}] sesión cerrada (logout)`);
        await this.authStore.delete(connectionId);
        await session.listener.onStatus(connectionId, 'disconnected');
        return;
      }

      // Cualquier otro motivo (caída de red, restart pedido por WhatsApp,
      // conflicto de sesión momentáneo, etc.) — a diferencia del Client de
      // whatsapp-web.js, un socket de Baileys es "desechable" y no
      // reconecta solo: hay que recrearlo. Backoff simple (no exponencial),
      // suficiente porque cada intento arma un socket nuevo desde cero.
      this.logger.warn(
        `[${connectionId}] conexión cerrada (código ${statusCode ?? 'desconocido'}: ${(lastDisconnect?.error as Error | undefined)?.message ?? 'sin detalle'}), reintentando...`,
      );
      // Mantener Redis/API al tanto: antes el estado se quedaba en el último
      // valor ('waiting_qr'/'connected') durante el reintento — y si el
      // reintento fallaba, para siempre.
      await session.listener.onStatus(connectionId, 'connecting');
      // 515 (restartRequired) es lo normal justo después de escanear el QR.
      const delayMs =
        statusCode === DisconnectReason.restartRequired ? 500 : 3000;
      setTimeout(() => {
        this.connect(connectionId, session.listener).catch(async (e) => {
          this.logger.error(
            `[${connectionId}] falló el reintento de conexión: ${e.message}`,
          );
          await session.listener.onStatus(connectionId, 'error');
        });
      }, delayMs);
    }
  }

  // ── construcción del estado de chats/contactos/mensajes ──────────

  private handleHistorySet(
    connectionId: string,
    session: BaileysConnectionSession,
    chats: any[],
    contacts: any[],
    messages: proto.IWebMessageInfo[],
  ): void {
    this.upsertChatsFromBaileys(session, chats);
    this.handleContactsUpsert(session, contacts);
    for (const msg of messages) {
      // 'history': historial re-sincronizado. Solo se cachea en memoria (lo
      // recoge el sync periódico); no se encola ni cuenta como no leído.
      this.handleIncomingMessage(connectionId, session, msg, 'history').catch(
        (e) =>
          this.logger.debug(
            `[${connectionId}] error cacheando mensaje de historial: ${e.message}`,
          ),
      );
    }
  }

  private upsertChatsFromBaileys(
    session: BaileysConnectionSession,
    chats: any[],
  ): void {
    for (const chat of chats ?? []) {
      if (!chat?.id) continue;
      // v7: un chat @lid puede traer su PN en `pnJid`; preferirlo mantiene un
      // único chatId por persona (el mismo que arma handleIncomingMessage).
      const canonical =
        isLidUser(chat.id) && chat.pnJid && isPnUser(chat.pnJid)
          ? chat.pnJid
          : chat.id;
      const legacyId = this.toLegacyId(jidNormalizedUser(canonical));
      if (!this.isRealChat(legacyId)) continue;

      const existing = session.chats.get(legacyId);
      const entry: InternalChatEntry = existing ?? {
        chatId: legacyId,
        name: this.jidUser(legacyId),
        isGroup: legacyId.endsWith('@g.us'),
        unreadCount: 0,
      };
      if (chat.name) entry.name = chat.name;
      if (typeof chat.unreadCount === 'number')
        entry.unreadCount = chat.unreadCount;
      if (chat.conversationTimestamp) {
        entry.lastMessageAt = Number(chat.conversationTimestamp);
      }
      session.chats.set(legacyId, entry);
    }
  }

  private handleContactsUpsert(
    session: BaileysConnectionSession,
    contacts: any[],
  ): void {
    for (const c of contacts ?? []) {
      if (!c?.id) continue;
      const pn = isLidUser(c.id) && c.phoneNumber ? c.phoneNumber : c.id;
      const legacyId = this.toLegacyId(jidNormalizedUser(pn));
      const existing = session.contacts.get(legacyId);
      const name = c.name || c.notify || existing?.name || this.jidUser(c.id);
      session.contacts.set(legacyId, {
        chatId: legacyId,
        name,
        phoneNumber: this.jidUser(c.id),
      });
    }
  }

  private async handleIncomingMessage(
    connectionId: string,
    session: BaileysConnectionSession,
    msg: proto.IWebMessageInfo,
    upsertType: string,
  ): Promise<void> {
    if (msg.messageStubType) return; // notificación del sistema (agregado al grupo, etc.), no un mensaje real

    const remoteJid = msg.key?.remoteJid;
    if (!remoteJid) return;

    // Dedupe: sendMessage() devuelve el mensaje Y Baileys lo re-emite como
    // 'append' (emitOwnEvents) — sin esto se cachea/encola dos veces.
    const messageIdRaw = msg.key.id || '';
    if (messageIdRaw) {
      if (session.seenMessageIds.has(messageIdRaw)) return;
      session.seenMessageIds.add(messageIdRaw);
      if (session.seenMessageIds.size > MAX_SEEN_MESSAGE_IDS) {
        const oldest = session.seenMessageIds.values().next().value;
        if (oldest) session.seenMessageIds.delete(oldest);
      }
    }

    // v7: remoteJid llega como @lid para la mayoría de chats 1:1. Sin esto
    // isRealChat() los descartaba TODOS en silencio.
    const legacyChatId = await this.resolveChatId(
      session,
      msg.key as WAMessageKey,
    );
    if (!this.isRealChat(legacyChatId)) return; // canal/difusión/estado, no es una persona real

    const content = this.unwrapContent(msg.message);
    if (!content || content.protocolMessage || content.reactionMessage) {
      return; // revocaciones, reacciones u otros mensajes "de control"
    }

    // Verificado en el código de Baileys (messages-recv): los mensajes que
    // llegaron mientras estabas desconectado (reinicio, reconexión, caída de
    // red) se emiten como 'append', y el eco de lo que enviamos también. Solo
    // el 'history' (messaging-history.set) es historial que NO debe encolarse.
    //  - 'notify'  → tiempo real: se persiste + evento en vivo.
    //  - 'append'  → offline/eco propio: se persiste (sin evento en vivo).
    //  - 'history' → solo caché en memoria.
    const isLiveMessage = upsertType === 'notify';
    const shouldPersist = upsertType === 'notify' || upsertType === 'append';

    const fromMe = !!msg.key.fromMe;
    const isGroup = legacyChatId.endsWith('@g.us');
    const { body, type, hasMedia } = this.extractMessageInfo(content);

    let author: string | undefined;
    let authorName: string | undefined;
    if (isGroup && !fromMe && msg.key.participant) {
      author = this.toLegacyId(
        this.pickPn(
          msg.key.participant,
          (msg.key as WAMessageKey).participantAlt,
        ),
      );
      authorName =
        msg.pushName ||
        session.contacts.get(author)?.name ||
        this.jidUser(author);
    }

    // En v7 las menciones pueden venir con nuestro LID, no solo con el PN.
    const myJids = [session.sock.user?.id, session.sock.user?.lid]
      .filter(Boolean)
      .map((j) => jidNormalizedUser(j as string));
    const mentionedJids: string[] =
      content.extendedTextMessage?.contextInfo?.mentionedJid ?? [];
    const mentionsMe = mentionedJids.some((jid) =>
      myJids.includes(jidNormalizedUser(jid)),
    );

    const messageId = msg.key.id || '';
    // Baileys no distingue "id corto" vs "id serializado" como
    // whatsapp-web.js (msg.id.id vs msg.id._serialized): acá messageId y
    // serializedId quedan iguales a propósito.
    const timestamp =
      typeof msg.messageTimestamp === 'number'
        ? msg.messageTimestamp
        : Number(
            msg.messageTimestamp?.toString?.() ?? Math.floor(Date.now() / 1000),
          );

    const parsed: WhatsappRawMessage = {
      messageId,
      fromMe,
      body,
      timestamp,
      // Nota de ACK: Baileys no expone en el mensaje entrante un ack
      // equivalente al de whatsapp-web.js (-1..4) de forma directa; el
      // estado de entrega/lectura llega aparte vía 'messages.update'. Igual
      // que la versión anterior (que tampoco escuchaba message_ack), esto
      // se deja en un valor fijo — no se trackea el ciclo de vida completo.
      ack: 1,
      isGroup,
      type,
      hasMedia,
      author,
      serializedId: messageId,
      mentionsMe,
    };
    this.cacheMessage(session, legacyChatId, msg, parsed);

    const chatEntry = this.upsertChatOnMessage(
      session,
      legacyChatId,
      isGroup,
      body,
      timestamp,
      fromMe,
      shouldPersist,
      msg.key,
      !fromMe && !isGroup ? msg.pushName : undefined,
    );

    if (!shouldPersist) return; // historial re-sincronizado: no se encola

    const persistPayload: WhatsappMessagePersistPayload = {
      connectionId,
      chatId: legacyChatId,
      chatName: chatEntry.name,
      messageId,
      fromMe,
      body,
      timestamp,
      ack: 1,
      unreadCount: chatEntry.unreadCount ?? 0,
      isGroup,
      type,
      hasMedia,
      author,
      authorName,
      mentionsMe,
      serializedId: messageId,
    };
    await session.listener.onMessagePersist(connectionId, persistPayload);

    // El evento en vivo ('message-received') solo para tiempo real, no para
    // mensajes offline/eco (esos igual llegan al front vía el persist processor).
    if (!isLiveMessage || fromMe) return;

    const contactId = isGroup ? (author ?? legacyChatId) : legacyChatId;
    const cachedContact = session.contacts.get(contactId);
    const phoneNumber = this.jidUser(contactId);
    await session.listener.onMessageReceivedLive(connectionId, {
      chatId: legacyChatId,
      isGroup,
      contact: {
        chatId: contactId,
        name: cachedContact?.name || msg.pushName || phoneNumber,
        phoneNumber,
      },
      text: body,
      messageId,
      serializedId: messageId,
      type,
      hasMedia,
    });
  }

  private upsertChatOnMessage(
    session: BaileysConnectionSession,
    legacyChatId: string,
    isGroup: boolean,
    body: string,
    timestamp: number,
    fromMe: boolean,
    countsAsNew: boolean,
    lastMessageKey?: proto.IMessageKey,
    pushName?: string,
  ): InternalChatEntry {
    const existing = session.chats.get(legacyChatId);
    const entry: InternalChatEntry = existing ?? {
      chatId: legacyChatId,
      name: this.jidUser(legacyChatId),
      isGroup,
      unreadCount: 0,
    };
    // Chat 1:1 todavía nombrado con el número/LID: usar el nombre que el
    // contacto tiene en su WhatsApp (pushName) si vino en el mensaje.
    if (pushName && entry.name === this.jidUser(legacyChatId)) {
      entry.name = pushName;
    }
    entry.lastMessage = body;
    entry.lastMessageAt = timestamp;
    entry.lastMessageKey = lastMessageKey;
    entry.lastMessageTimestamp = timestamp;
    // Aproximación: a diferencia de `chat.unreadCount` de whatsapp-web.js
    // (ya calculado por el propio WhatsApp Web), Baileys no da un contador
    // confiable por cada mensaje individual — se lleva a mano acá y se
    // corrige con lo que reporten 'chats.upsert'/'chats.update' cuando
    // WhatsApp los envíe. PENDIENTE A PROPÓSITO, mismo criterio que otros
    // puntos ya documentados como incompletos en esta migración.
    if (countsAsNew && !fromMe) {
      entry.unreadCount = (entry.unreadCount ?? 0) + 1;
    }
    session.chats.set(legacyChatId, entry);
    return entry;
  }

  private async persistOutgoing(
    connectionId: string,
    session: BaileysConnectionSession,
    sent: proto.IWebMessageInfo | undefined,
  ): Promise<void> {
    if (!sent?.key?.remoteJid) return;
    try {
      // Reutiliza el mismo camino que un mensaje entrante: `fromMe` ya
      // viene en `true` desde `sock.sendMessage()`, así que persiste pero
      // no dispara 'message-received' (ver el `if (fromMe) return` de
      // arriba). Si Baileys además re-emite este mismo mensaje por su
      // cuenta vía 'messages.upsert' (pasa en algunas versiones, por el eco
      // multi-dispositivo), el duplicado es inofensivo: el processor de
      // persistencia del lado API inserta con `.orIgnore()`.
      await this.handleIncomingMessage(connectionId, session, sent, 'notify');
    } catch (e) {
      this.logger.warn(
        `[${connectionId}] no se pudo procesar el mensaje saliente para persistir: ${e.message}`,
      );
    }
  }

  // ── media / envío ────────────────────────────────────────────────

  private async resolveSendTarget(
    session: BaileysConnectionSession,
    chatId: string,
  ): Promise<string> {
    // baileys.wiki (JIDs / v7 migration): a un usuario se le puede escribir
    // con su LID o con su PN; onWhatsApp() es para números de teléfono.
    if (isLidUser(chatId)) return chatId;
    return this.toBaileysJid(chatId);
  }

  private async loadImageContent(
    url: string,
  ): Promise<{ image: Buffer | { url: string } }> {
    const isLocal = !url.startsWith('http://') && !url.startsWith('https://');
    if (isLocal) {
      return { image: await fs.readFile(url) };
    }
    // Baileys descarga la URL remota por su cuenta al enviar.
    return { image: { url } };
  }

  private async getGroupMetadataCached(
    session: BaileysConnectionSession,
    jid: string,
    forceRefresh = false,
  ): Promise<any | undefined> {
    if (!forceRefresh && session.groupMetadataCache.has(jid)) {
      return session.groupMetadataCache.get(jid);
    }
    if (Date.now() < (session.groupMetaBlockedUntil.get(jid) ?? 0)) {
      return session.groupMetadataCache.get(jid); // bloqueado tras un fallo reciente
    }
    try {
      const metadata = await session.sock.groupMetadata(jid);
      session.groupMetadataCache.set(jid, metadata);
      return metadata;
    } catch (e) {
      const forbidden =
        /forbidden/i.test(e?.message ?? '') || e?.output?.statusCode === 403;
      session.groupMetaBlockedUntil.set(
        jid,
        Date.now() +
          (forbidden ? GROUP_FORBIDDEN_BLOCK_MS : GROUP_RATE_LIMIT_BLOCK_MS),
      );
      this.logger.debug(
        `No se pudo obtener metadata del grupo ${jid}: ${e.message} (no se reintenta por ${forbidden ? '6 h' : '10 min'})`,
      );
      return undefined;
    }
  }

  /**
   * Trae la metadata de TODOS los grupos en una sola petición
   * (`groupFetchAllParticipating`), como mucho una vez cada GROUPS_REFRESH_MS,
   * y comparte la petición en curso entre llamadas concurrentes.
   */
  private refreshAllGroups(session: BaileysConnectionSession): Promise<void> {
    if (session.groupsFetchInFlight) return session.groupsFetchInFlight;
    if (Date.now() - session.lastGroupsFetch < GROUPS_REFRESH_MS) {
      return Promise.resolve();
    }
    session.groupsFetchInFlight = (async () => {
      try {
        const all = await session.sock.groupFetchAllParticipating();
        for (const [jid, metadata] of Object.entries(all ?? {})) {
          session.groupMetadataCache.set(jid, metadata);
          session.groupMetaBlockedUntil.delete(jid);
        }
        session.lastGroupsFetch = Date.now();
      } catch (e) {
        // Reintento en ~1 min, no en el siguiente sync.
        session.lastGroupsFetch = Date.now() - GROUPS_REFRESH_MS + 60_000;
        this.logger.warn(
          `groupFetchAllParticipating falló: ${e?.message ?? e}`,
        );
      } finally {
        session.groupsFetchInFlight = undefined;
      }
    })();
    return session.groupsFetchInFlight;
  }

  /**
   * Implementación mínima de `CacheStore` (get/set/del/flushAll — la misma
   * interfaz que usan `msgRetryCounterCache` y compañía en `SocketConfig`)
   * sin sumar `@cacheable/node-cache` como dependencia nueva. Basta para
   * este uso puntual: solo lo consume Baileys internamente para decidir si
   * reintentar un mensaje fallido.
   */
  private createMemoryCacheStore(): {
    get: <T>(key: string) => T | undefined;
    set: <T>(key: string, value: T) => void;
    del: (key: string) => void;
    flushAll: () => void;
  } {
    const store = new Map<string, unknown>();
    return {
      get: <T>(key: string) => store.get(key) as T | undefined,
      set: <T>(key: string, value: T) => {
        store.set(key, value);
      },
      del: (key: string) => {
        store.delete(key);
      },
      flushAll: () => store.clear(),
    };
  }

  private cacheMessage(
    session: BaileysConnectionSession,
    chatId: string,
    raw: proto.IWebMessageInfo,
    parsed: WhatsappRawMessage,
  ): void {
    if (raw.key?.id) {
      session.rawMessagesById.set(raw.key.id, raw);
      if (session.rawMessagesById.size > MAX_CACHED_RAW_MESSAGES) {
        const oldestKey = session.rawMessagesById.keys().next().value;
        if (oldestKey) session.rawMessagesById.delete(oldestKey);
      }
    }

    const list = session.messagesByChat.get(chatId) ?? [];
    list.push(parsed);
    if (list.length > MAX_MESSAGES_PER_CHAT) list.shift();
    session.messagesByChat.set(chatId, list);
  }

  // ── helpers de contenido de mensaje ──────────────────────────────

  private unwrapContent(
    content?: proto.IMessage | null,
  ): proto.IMessage | undefined {
    if (!content) return undefined;
    if (content.ephemeralMessage?.message) {
      return this.unwrapContent(content.ephemeralMessage.message);
    }
    if (content.viewOnceMessage?.message) {
      return this.unwrapContent(content.viewOnceMessage.message);
    }
    if ((content as any).viewOnceMessageV2?.message) {
      return this.unwrapContent((content as any).viewOnceMessageV2.message);
    }
    if ((content as any).documentWithCaptionMessage?.message) {
      return this.unwrapContent(
        (content as any).documentWithCaptionMessage.message,
      );
    }
    return content;
  }

  private extractMessageInfo(content: proto.IMessage): {
    body: string;
    type: WhatsappMessageType;
    hasMedia: boolean;
  } {
    if (content.conversation) {
      return { body: content.conversation, type: 'chat', hasMedia: false };
    }
    if (content.extendedTextMessage) {
      return {
        body: content.extendedTextMessage.text || '',
        type: 'chat',
        hasMedia: false,
      };
    }
    if (content.imageMessage) {
      return {
        body: content.imageMessage.caption || '',
        type: 'image',
        hasMedia: true,
      };
    }
    if (content.videoMessage) {
      return {
        body: content.videoMessage.caption || '',
        type: 'video',
        hasMedia: true,
      };
    }
    if (content.audioMessage) {
      return {
        body: '',
        type: content.audioMessage.ptt ? 'ptt' : 'audio',
        hasMedia: true,
      };
    }
    if (content.documentMessage) {
      return {
        body: content.documentMessage.caption || '',
        type: 'document',
        hasMedia: true,
      };
    }
    if (content.stickerMessage) {
      return { body: '', type: 'sticker', hasMedia: true };
    }
    if (content.locationMessage) {
      return { body: '', type: 'location', hasMedia: false };
    }
    if (content.contactMessage || content.contactsArrayMessage) {
      return { body: '', type: 'vcard', hasMedia: false };
    }
    return { body: '', type: 'unknown', hasMedia: false };
  }

  private extractMimetype(content?: proto.IMessage): string | undefined {
    return (
      content?.imageMessage?.mimetype ||
      content?.videoMessage?.mimetype ||
      content?.audioMessage?.mimetype ||
      content?.documentMessage?.mimetype ||
      content?.stickerMessage?.mimetype ||
      undefined
    );
  }

  // ── helpers de JID / sesión ──────────────────────────────────────

  /**
   * chatId canónico (formato legacy) de un mensaje. Si el chat viene como LID
   * se intenta resolver a su número (remoteJidAlt o el mapping cacheado por
   * Baileys); si no se conoce el número, el chat queda como `<lid>@lid` — que
   * también se puede usar para enviar (ver resolveSendTarget).
   */
  private async resolveChatId(
    session: BaileysConnectionSession,
    key: WAMessageKey,
  ): Promise<string> {
    const jid = key.remoteJid ?? '';
    if (isLidUser(jid)) {
      let pn: string | null | undefined = key.remoteJidAlt;
      if (!pn || !isPnUser(pn)) {
        pn = await session.sock.signalRepository.lidMapping
          .getPNForLID(jid)
          .catch(() => null);
      }
      if (pn && isPnUser(pn)) return this.toLegacyId(jidNormalizedUser(pn));
      return jidNormalizedUser(jid);
    }
    return this.toLegacyId(jidNormalizedUser(jid));
  }

  /** Para participantes de grupo: prefiere el PN si el principal es un LID. */
  private pickPn(primary?: string | null, alt?: string | null): string {
    if (primary && isLidUser(primary) && alt && isPnUser(alt)) return alt;
    return primary ?? alt ?? '';
  }

  private toLegacyId(jid?: string | null): string {
    if (!jid) return '';
    if (jid.endsWith(BAILEYS_INDIVIDUAL_SUFFIX)) {
      return (
        jid.slice(0, -BAILEYS_INDIVIDUAL_SUFFIX.length) +
        LEGACY_INDIVIDUAL_SUFFIX
      );
    }
    return jid;
  }

  private toBaileysJid(legacyId: string): string {
    if (legacyId.endsWith(LEGACY_INDIVIDUAL_SUFFIX)) {
      return (
        legacyId.slice(0, -LEGACY_INDIVIDUAL_SUFFIX.length) +
        BAILEYS_INDIVIDUAL_SUFFIX
      );
    }
    return legacyId;
  }

  private jidUser(jid: string): string {
    return jid.split('@')[0]?.split(':')[0] ?? jid;
  }

  /**
   * true solo para chats 1:1 (`@c.us`, ya normalizado) y grupos (`@g.us`) —
   * mismo criterio que `isRealChat` tenía en whatsapp-runtime.service.ts
   * para excluir canales (`@newsletter`), difusión (`@broadcast`) y
   * estados (`status@broadcast`).
   */
  private isRealChat(legacyId: string): boolean {
    return (
      legacyId.endsWith('@c.us') ||
      legacyId.endsWith('@g.us') ||
      legacyId.endsWith('@lid') // v7: chat 1:1 cuyo número aún no se conoce
    );
  }

  private getConnectedSession(
    connectionId: string,
  ): BaileysConnectionSession | null {
    const session = this.sessions.get(connectionId);
    // `sock.user` solo se puebla una vez autenticado y con la conexión
    // abierta — es el análogo más cercano a `client.getState() === 'CONNECTED'`.
    return session?.sock?.user ? session : null;
  }

  private requireConnectedSession(
    connectionId: string,
  ): BaileysConnectionSession {
    const session = this.getConnectedSession(connectionId);
    if (!session) throw new Error('WhatsApp no está conectado');
    return session;
  }
}
