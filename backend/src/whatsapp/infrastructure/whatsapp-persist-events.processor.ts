import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Job } from 'bullmq';
import { EntityManager, Repository } from 'typeorm';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { RealtimeGateway } from 'src/realtime/realtime.gateway';
import {
  WHATSAPP_HISTORY_BATCH_JOB_NAME,
  WHATSAPP_HISTORY_DONE_JOB_NAME,
  WHATSAPP_PERSIST_EVENT_JOB_NAME,
  WHATSAPP_PERSIST_EVENTS_QUEUE,
  WhatsappHistoryBatchJob,
  WhatsappHistoryDoneJob,
  WhatsappMessagePersistPayload,
  WhatsappPersistEventJob,
} from 'shared/whatsapp-contracts';
import { WhatsappConnectionsService } from '../services/whatsapp-connections.service';
import { pickChatName } from '../services/whatsapp-chat-name';
import { WhatsappSyncQueue } from './jobs/whatsapp-sync.queue';

/**
 * Máximo de mensajes por INSERT. Postgres admite 65 535 parámetros por
 * consulta y cada mensaje usa ~15 columnas: un solo INSERT con más de ~4 300
 * mensajes falla completo. Un batch de historial puede traer miles.
 */
const INSERT_CHUNK = 500;

interface ConversationResult {
  /** El chat/grupo no existía antes de este upsert. */
  isNew: boolean;
  name: string;
  lastMessage: string | null;
  lastMessageAt: number | null;
}

/**
 * Consume la cola BullMQ `whatsapp-persist-events` (la produce el runtime).
 * Maneja tres tipos de job:
 *
 *  - `message-persist`      → un mensaje en vivo (`processSingleMessage`).
 *  - `history-batch-persist`→ un lote de historial (`processHistoryBatch`).
 *  - `history-sync-done`    → el runtime terminó de importar el historial.
 *
 * `processSingleMessage` estaba como stub (`throw new Error('Method not
 * implemented.')`): TODO mensaje en vivo fallaba, BullMQ lo reintentaba 4
 * veces y lo descartaba sin llegar nunca a la BD. Su lógica es:
 *
 *  - Inserta el mensaje con `orIgnore` (idempotente ante reintentos/ecos).
 *  - Actualiza el último mensaje del chat/grupo SIN pisar uno más nuevo (un
 *    evento fuera de orden respecto al sync no debe hacer "saltar" el chat).
 *  - Si el chat/grupo es nuevo → `emitNewChat` / `emitNewGroup` de inmediato,
 *    sin esperar al siguiente sync (hasta 3 min antes). Al crear un grupo se
 *    completa `whatsappConnectionId`, si no `findAllById` no lo encuentra.
 *  - `emitChatUpdated` SIEMPRE que se inserta un mensaje nuevo (propio o
 *    ajeno), para que otras pestañas/dispositivos lo vean en tiempo real.
 *  - `emitNewMessages` (badge de no leídos) solo para mensajes AJENOS, con
 *    el total de chats + grupos (`getUnreadTotal`).
 *  - Guardado + actualización del chat van en UNA transacción: si algo falla
 *    a mitad, el reintento de BullMQ parte de cero (sin dejar el mensaje
 *    guardado pero el chat sin actualizar, ni contar el no-leído dos veces).
 *    Los eventos de socket se emiten DESPUÉS del commit.
 */
@Injectable()
@Processor(WHATSAPP_PERSIST_EVENTS_QUEUE)
export class WhatsappPersistEventsProcessor extends WorkerHost {
  private readonly logger = new Logger(WhatsappPersistEventsProcessor.name);

  constructor(
    @InjectRepository(WhatsappChat)
    private readonly chatRepo: Repository<WhatsappChat>,
    @InjectRepository(WhatsappMessage)
    private readonly messageRepo: Repository<WhatsappMessage>,
    @InjectRepository(WhatsappGroup)
    private readonly groupRepo: Repository<WhatsappGroup>,
    private readonly gateway: RealtimeGateway,
    private readonly whatsappConnectionsService: WhatsappConnectionsService,
    private readonly syncQueue: WhatsappSyncQueue,
  ) {
    super();
  }

  async process(job: Job<any>) {
    switch (job.name) {
      case WHATSAPP_HISTORY_BATCH_JOB_NAME:
        return this.processHistoryBatch(job.data as WhatsappHistoryBatchJob);
      case WHATSAPP_HISTORY_DONE_JOB_NAME:
        return this.processHistoryDone(job.data as WhatsappHistoryDoneJob);
      case WHATSAPP_PERSIST_EVENT_JOB_NAME:
        return this.processSingleMessage(job.data as WhatsappPersistEventJob);
      default:
        // Compatibilidad con jobs viejos que quedaran en Redis sin nombre
        // conocido: si traen un mensaje se guarda; si no, se descarta en
        // vez de lanzar (un throw solo provocaría 4 reintentos inútiles).
        if (job.data?.payload?.messageId) {
          return this.processSingleMessage(job.data as WhatsappPersistEventJob);
        }
        this.logger.warn(`Job desconocido "${job.name}" descartado.`);
    }
  }

  // ── mensaje en vivo ────────────────────────────────────────────────

  private async processSingleMessage(data: WhatsappPersistEventJob) {
    const m = data?.payload;
    if (!m?.messageId || !m.chatId || !m.connectionId) {
      this.logger.warn('message-persist sin payload válido; se descarta.');
      return;
    }

    const connection = await this.whatsappConnectionsService
      .findByConnectionId(m.connectionId)
      .catch(() => null);
    const dbConnectionId = connection?.id ?? null;

    const stored = await this.messageRepo.manager.transaction(async (em) => {
      const msgRepo = em.getRepository(WhatsappMessage);
      const alreadyStored = await msgRepo.exists({
        where: { sessionId: m.connectionId, messageId: m.messageId },
      });
      if (!alreadyStored) {
        await this.insertMessages(em, [m]);
      }

      // Aunque el mensaje ya estuviera (reintento), se reaplica el último
      // mensaje: es idempotente y repara un reintento tras fallo parcial.
      const convo = await this.upsertConversation(em, m, dbConnectionId);

      const countsAsUnread = !alreadyStored && !m.fromMe;
      if (countsAsUnread) await this.incrementUnread(em, m);

      return {
        alreadyStored,
        convo,
        unreadCount: await this.readUnread(em, m),
      };
    });

    // Reintento o eco de un mensaje ya guardado: no se vuelve a notificar.
    if (stored.alreadyStored) return;

    try {
      const { convo, unreadCount } = stored;
      if (convo.isNew) {
        if (m.isGroup) {
          this.gateway.emitNewGroup(m.connectionId, {
            whatsappGroupId: m.chatId,
            title: convo.name,
            unreadCount,
          });
        } else {
          this.gateway.emitNewChat(m.connectionId, {
            chatId: m.chatId,
            name: convo.name,
            unreadCount,
          });
        }
      }

      this.gateway.emitChatUpdated(m.connectionId, {
        chatId: m.chatId,
        isGroup: m.isGroup,
        name: convo.name,
        lastMessage: convo.lastMessage ?? m.body,
        lastMessageAt: convo.lastMessageAt ?? m.timestamp,
        unreadCount,
        fromMe: m.fromMe,
      });

      if (!m.fromMe) {
        const total = await this.getUnreadTotal(m.connectionId);
        this.gateway.emitNewMessages(m.connectionId, m.chatId, 1, total);
      }
    } catch (e) {
      // El mensaje YA está en la BD: un fallo de socket no debe reintentar
      // el job (no aporta nada y solo repetiría trabajo).
      this.logger.warn(
        `Mensaje ${m.messageId} guardado, pero falló la notificación: ${e?.message ?? e}`,
      );
    }
  }

  // ── historial ──────────────────────────────────────────────────────

  private async processHistoryBatch(data: WhatsappHistoryBatchJob) {
    const messages = data?.messages ?? [];
    if (!messages.length) return;

    const connectionCache = new Map<string, string | null>();
    const dbConnectionIdFor = async (connectionId: string) => {
      if (!connectionCache.has(connectionId)) {
        const c = await this.whatsappConnectionsService
          .findByConnectionId(connectionId)
          .catch(() => null);
        connectionCache.set(connectionId, c?.id ?? null);
      }
      return connectionCache.get(connectionId)!;
    };

    // un batch de historial trae mensajes de varios chats mezclados —
    // nos quedamos con el más nuevo POR CHAT dentro de este batch.
    const latestByChat = new Map<string, WhatsappMessagePersistPayload>();
    for (const m of messages) {
      const key = `${m.connectionId}|${m.chatId}`;
      const current = latestByChat.get(key);
      if (!current || m.timestamp > current.timestamp) latestByChat.set(key, m);
    }

    const created = await this.messageRepo.manager.transaction(async (em) => {
      await this.insertMessages(em, messages);

      const newConversations: {
        last: WhatsappMessagePersistPayload;
        name: string;
      }[] = [];
      for (const last of latestByChat.values()) {
        const convo = await this.upsertConversation(
          em,
          last,
          await dbConnectionIdFor(last.connectionId),
        );
        if (convo.isNew) newConversations.push({ last, name: convo.name });
      }
      return newConversations;
    });

    // Los eventos de socket salen DESPUÉS del commit: si la transacción
    // falla no se anuncian chats que no existen.
    for (const { last, name } of created) {
      if (last.isGroup) {
        this.gateway.emitNewGroup(last.connectionId, {
          whatsappGroupId: last.chatId,
          title: name,
          unreadCount: 0,
        });
      } else {
        this.gateway.emitNewChat(last.connectionId, {
          chatId: last.chatId,
          name,
          unreadCount: 0,
        });
      }
    }
  }

  /**
   * El runtime ya envió TODO el historial: primero todos los lotes de chats
   * 1:1 y después todos los de grupos (la cola es FIFO). Falta reconciliar
   * con el estado del runtime (chats sin mensajes, nombres, no leídos) y
   * eso lo hace el sync completo, que además emite por WebSocket
   * `whatsapp:chats-synced` cuando terminan los chats y, después,
   * `whatsapp:groups-synced` cuando terminan los grupos.
   */
  private async processHistoryDone(data: WhatsappHistoryDoneJob) {
    const { connectionId, totalChats, totalGroups } = data.payload;
    this.logger.log(
      `[${connectionId}] historial recibido (${totalChats} chats, ${totalGroups} grupos en memoria del runtime); lanzando sync completo.`,
    );
    await this.syncQueue.enqueueFullSync(connectionId);
  }

  // ── piezas compartidas ─────────────────────────────────────────────

  private async insertMessages(
    em: EntityManager,
    messages: WhatsappMessagePersistPayload[],
  ) {
    const rows = messages.map((m) => ({
      sessionId: m.connectionId,
      chatId: m.chatId,
      messageId: m.messageId,
      fromMe: m.fromMe,
      body: m.body,
      timestamp: m.timestamp,
      isRead: m.fromMe,
      ack: m.ack,
      isGroup: m.isGroup,
      type: m.type,
      hasMedia: m.hasMedia,
      author: m.author,
      authorName: m.authorName,
      mentionsMe: m.mentionsMe,
      serializedId: m.serializedId,
    }));

    for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
      await em
        .getRepository(WhatsappMessage)
        .createQueryBuilder()
        .insert()
        .into(WhatsappMessage)
        .values(rows.slice(i, i + INSERT_CHUNK))
        .orIgnore()
        .execute();
    }
  }

  /**
   * Upsert del chat o grupo con el último mensaje `last`, conservando lo ya
   * guardado si es más nuevo (evento fuera de orden) y sin degradar el nombre.
   */
  private async upsertConversation(
    em: EntityManager,
    last: WhatsappMessagePersistPayload,
    dbConnectionId: string | null,
  ): Promise<ConversationResult> {
    if (last.isGroup) {
      const repo = em.getRepository(WhatsappGroup);
      const existing = await repo.findOne({
        where: { whatsappGroupId: last.chatId },
        select: ['whatsappGroupId', 'title', 'lastMessage', 'lastMessageAt'],
      });
      const stale =
        !!existing?.lastMessageAt &&
        Number(existing.lastMessageAt) > last.timestamp;
      const title = pickChatName(existing?.title, last.chatName, last.chatId);
      const lastMessage = stale ? existing!.lastMessage : last.body;
      const lastMessageAt = stale
        ? Number(existing!.lastMessageAt)
        : last.timestamp;

      await repo.upsert(
        {
          whatsappGroupId: last.chatId,
          title,
          lastMessage,
          lastMessageAt,
          // solo se escribe si se conoce: no pisar con null uno ya guardado
          ...(dbConnectionId ? { whatsappConnectionId: dbConnectionId } : {}),
        },
        ['whatsappGroupId'],
      );
      return { isNew: !existing, name: title, lastMessage, lastMessageAt };
    }

    const repo = em.getRepository(WhatsappChat);
    const existing = await repo.findOne({
      where: { sessionId: last.connectionId, chatId: last.chatId },
      select: ['isNew', 'name', 'lastMessageAt', 'lastMessage'],
    });
    const stale =
      !!existing?.lastMessageAt &&
      Number(existing.lastMessageAt) > last.timestamp;
    const name = pickChatName(existing?.name, last.chatName, last.chatId);
    const lastMessage = stale ? existing!.lastMessage : last.body;
    const lastMessageAt = stale
      ? Number(existing!.lastMessageAt)
      : last.timestamp;

    await repo.upsert(
      {
        sessionId: last.connectionId,
        chatId: last.chatId,
        name,
        lastMessage,
        lastMessageAt,
        isNew: existing ? existing.isNew : true,
      },
      ['sessionId', 'chatId'],
    );
    return { isNew: !existing, name, lastMessage, lastMessageAt };
  }

  private async incrementUnread(
    em: EntityManager,
    m: WhatsappMessagePersistPayload,
  ) {
    if (m.isGroup) {
      await em
        .getRepository(WhatsappGroup)
        .increment({ whatsappGroupId: m.chatId }, 'unreadCount', 1);
    } else {
      await em
        .getRepository(WhatsappChat)
        .increment(
          { sessionId: m.connectionId, chatId: m.chatId },
          'unreadCount',
          1,
        );
    }
  }

  private async readUnread(
    em: EntityManager,
    m: WhatsappMessagePersistPayload,
  ): Promise<number> {
    const row = m.isGroup
      ? await em.getRepository(WhatsappGroup).findOne({
          where: { whatsappGroupId: m.chatId },
          select: ['unreadCount'],
        })
      : await em.getRepository(WhatsappChat).findOne({
          where: { sessionId: m.connectionId, chatId: m.chatId },
          select: ['unreadCount'],
        });
    return Number(row?.unreadCount ?? 0);
  }

  /**
   * Suma de no leídos de chats individuales + grupos de esta conexión.
   * Antes solo se sumaba whatsapp_chat, así que un mensaje de grupo nunca
   * se reflejaba en el total emitido por socket.
   */
  private async getUnreadTotal(connectionId: string): Promise<number> {
    const { total: chatsTotal } = await this.chatRepo
      .createQueryBuilder('c')
      .select('COALESCE(SUM(c.unreadCount), 0)', 'total')
      .where('c.sessionId = :sessionId', { sessionId: connectionId })
      .getRawOne();

    const connection = await this.whatsappConnectionsService
      .findByConnectionId(connectionId)
      .catch(() => null);

    let groupsTotal = 0;
    if (connection) {
      const raw = await this.groupRepo
        .createQueryBuilder('g')
        .select('COALESCE(SUM(g.unreadCount), 0)', 'total')
        .where('g.whatsappConnectionId = :whatsappConnectionId', {
          whatsappConnectionId: connection.id,
        })
        .getRawOne();
      groupsTotal = Number(raw.total);
    }

    return Number(chatsTotal) + groupsTotal;
  }
}
