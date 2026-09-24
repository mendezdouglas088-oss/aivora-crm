import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { RealtimeGateway } from 'src/realtime/realtime.gateway';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { WhatsappConnectionsService } from '../services/whatsapp-connections.service';
import { WhatsappCommandsService } from '../services/whatsapp-commands.service';
import { pickChatName } from '../services/whatsapp-chat-name';
import { WhatsappChatSummary } from 'shared/whatsapp-contracts';

/**
 * --- Fixes de la ronda de revisión (items 4 y 5) ---
 *  (4) `existingChatsMap`/`existingGroupsMap` ahora también traen
 *      `lastMessageAt`/`lastMessage`. Antes el upsert escribía a ciegas lo
 *      que devolvía el sync; si el sync es más lento que un evento en vivo
 *      que ya llegó, podía pisar un lastMessageAt más nuevo con uno viejo
 *      y hacer que el chat/grupo "saltara" de posición. Ahora se conserva
 *      lo que ya había si es más reciente que lo que trae este sync.
 *  (5) `getUnreadTotal` reemplaza el cálculo que solo sumaba
 *      whatsapp_chat: ahora suma también los grupos de la conexión.
 */
/**
 * --- Orden y notificaciones ---
 *  `syncAll` ahora procesa PRIMERO todos los chats 1:1 y DESPUÉS todos los
 *  grupos. Con `notify: true` emite por WebSocket `whatsapp:chats-synced`
 *  al terminar los chats y `whatsapp:groups-synced` al terminar los grupos
 *  (dos avisos separados). Sin `notify` (refrescos en background) no emite
 *  ninguno, para no provocar bucles con un frontend que recarga al recibirlos.
 *
 * --- Chats duplicados (LID / PN) ---
 *  Si el runtime informa `aliases` (ids `@lid` de la misma persona), los
 *  chats duplicados ya guardados en la BD se fusionan en el canónico.
 */
@Injectable()
export class WhatsappSyncService {
  private readonly logger = new Logger(WhatsappSyncService.name);

  constructor(
    @InjectRepository(WhatsappChat)
    private readonly chatRepo: Repository<WhatsappChat>,
    @InjectRepository(WhatsappMessage)
    private readonly messageRepo: Repository<WhatsappMessage>,
    private readonly commandsService: WhatsappCommandsService,
    private readonly gateway: RealtimeGateway,
    @InjectRepository(WhatsappGroup)
    private readonly groupRepo: Repository<WhatsappGroup>,
    private readonly whatsappConnectionsService: WhatsappConnectionsService,
  ) {}

  async syncAll(sessionId: string, opts: { notify?: boolean } = {}) {
    // sessionId aquí ES connectionId — se mantiene el nombre por las
    // columnas de BD (WhatsappChat.sessionId), no por el contrato con el runtime.
    let allChats: Awaited<ReturnType<WhatsappCommandsService['syncAll']>>;
    try {
      allChats = await this.commandsService.syncAll(sessionId);
    } catch (e) {
      this.logger.debug(`Sync omitido para ${sessionId}: ${e.message}`);
      return; // no hay conexión activa o falló el comando — nada que sincronizar
    }

    const chats = allChats.filter((c) => !c.isGroup);
    const groups = allChats.filter((c) => c.isGroup);

    const connection =
      await this.whatsappConnectionsService.findByConnectionId(sessionId);
    const whatsappConnectionId = connection?.id ?? null;

    // Solo se notifica si el runtime devolvió algo: con la memoria del
    // runtime vacía (recién conectado, historial aún sin llegar) avisar
    // "sincronizado" con 0 chats sería falso.
    const notify = !!opts.notify && allChats.length > 0;

    // 0) duplicados ya guardados (mismo contacto como @lid y como @c.us)
    await this.mergeDuplicateChats(sessionId, chats);

    // 1) CHATS primero
    await this.syncConversations(sessionId, chats, whatsappConnectionId);
    if (notify)
      this.gateway.emitChatsSynced(sessionId, { total: chats.length });

    // 2) GRUPOS después
    await this.syncConversations(sessionId, groups, whatsappConnectionId);
    if (notify)
      this.gateway.emitGroupsSynced(sessionId, { total: groups.length });
  }

  /** Sincroniza una lista homogénea (solo chats o solo grupos) de summaries. */
  private async syncConversations(
    sessionId: string,
    list: WhatsappChatSummary[],
    whatsappConnectionId: string | null,
  ) {
    if (!list.length) return;

    const chatIds = list.filter((c) => !c.isGroup).map((c) => c.chatId);
    const groupIds = list.filter((c) => c.isGroup).map((c) => c.chatId);

    const existingChats = chatIds.length
      ? await this.chatRepo.find({
          where: { sessionId, chatId: In(chatIds) },
          select: ['chatId', 'name', 'isNew', 'lastMessageAt', 'lastMessage'],
        })
      : [];
    const existingChatsMap = new Map(existingChats.map((c) => [c.chatId, c]));

    const existingGroups = groupIds.length
      ? await this.groupRepo.find({
          where: { whatsappGroupId: In(groupIds) },
          select: ['whatsappGroupId', 'title', 'lastMessageAt', 'lastMessage'],
        })
      : [];
    const existingGroupsMap = new Map(
      existingGroups.map((g) => [g.whatsappGroupId, g]),
    );

    for (const c of list) {
      const existingChat = !c.isGroup
        ? existingChatsMap.get(c.chatId)
        : undefined;
      const existingGroup = c.isGroup
        ? existingGroupsMap.get(c.chatId)
        : undefined;
      const isNewGroup = c.isGroup && !existingGroup;
      const isNewChat = !c.isGroup && !existingChat;

      // No pisar un lastMessageAt más nuevo (guardado por un evento en
      // vivo) con el dato de este sync si viene más viejo/desactualizado.
      const existing = c.isGroup ? existingGroup : existingChat;
      const isStale =
        !!existing?.lastMessageAt &&
        !!c.lastMessageAt &&
        existing.lastMessageAt > c.lastMessageAt;
      const effectiveLastMessage = isStale
        ? existing!.lastMessage
        : c.lastMessage;
      const effectiveLastMessageAt = isStale
        ? existing!.lastMessageAt
        : c.lastMessageAt;

      if (c.isGroup) {
        await this.groupRepo.upsert(
          {
            whatsappGroupId: c.chatId,
            title: pickChatName(existingGroup?.title, c.name, c.chatId),
            lastMessage: effectiveLastMessage,
            lastMessageAt: effectiveLastMessageAt,
            unreadCount: c.unreadCount,
            participantsCount: c.participantsCount,
            whatsappConnectionId,
          },
          ['whatsappGroupId'],
        );
        if (isNewGroup) {
          this.gateway.emitNewGroup(sessionId, {
            whatsappGroupId: c.chatId,
            title: pickChatName(undefined, c.name, c.chatId),
            unreadCount: c.unreadCount,
          });
        }
      } else {
        await this.chatRepo.upsert(
          [
            {
              sessionId,
              chatId: c.chatId,
              name: pickChatName(existingChat?.name, c.name, c.chatId),
              lastMessage: effectiveLastMessage,
              lastMessageAt: effectiveLastMessageAt,
              unreadCount: c.unreadCount,
              isSavedContact: c.isSavedContact ?? null,
              isNew: existingChat ? existingChat.isNew : true,
            },
          ],
          ['sessionId', 'chatId'],
        );
        if (isNewChat) {
          this.gateway.emitNewChat(sessionId, {
            chatId: c.chatId,
            name: pickChatName(undefined, c.name, c.chatId),
            unreadCount: c.unreadCount,
          });
        }
      }

      const { newCount } = await this.syncMessagesForChat(sessionId, c.chatId);
      try {
        if (newCount > 0) {
          const total = await this.getUnreadTotal(
            sessionId,
            whatsappConnectionId,
          );

          this.gateway.emitNewMessages(sessionId, c.chatId, newCount, total);
        }
      } catch (error) {
        console.log(
          `Error al emitir evento de nuevos mensajes para ${sessionId} - ${c.chatId}:`,
          error,
        );
      }
    }
  }

  /**
   * Fusiona en el chat canónico (`@c.us`) los chats duplicados que la BD
   * pueda tener bajo su alias `@lid`: mueve sus mensajes y borra el chat
   * sobrante. En los syncs siguientes no encuentra nada y solo cuesta 2
   * consultas por lote.
   */
  private async mergeDuplicateChats(
    sessionId: string,
    chats: WhatsappChatSummary[],
  ) {
    const canonicalByAlias = new Map<string, string>();
    for (const c of chats) {
      for (const alias of c.aliases ?? []) {
        if (alias !== c.chatId) canonicalByAlias.set(alias, c.chatId);
      }
    }
    if (!canonicalByAlias.size) return;

    const aliasIds = [...canonicalByAlias.keys()];
    const withRows = new Set<string>();
    for (let i = 0; i < aliasIds.length; i += 500) {
      const ids = aliasIds.slice(i, i + 500);
      const chatRows = await this.chatRepo.find({
        where: { sessionId, chatId: In(ids) },
        select: ['chatId'],
      });
      chatRows.forEach((r) => withRows.add(r.chatId));
      const msgRows = await this.messageRepo
        .createQueryBuilder('m')
        .select('m.chatId', 'chatId')
        .where('m.sessionId = :sessionId', { sessionId })
        .andWhere('m.chatId IN (:...ids)', { ids })
        .groupBy('m.chatId')
        .getRawMany<{ chatId: string }>();
      msgRows.forEach((r) => withRows.add(r.chatId));
    }

    for (const aliasId of withRows) {
      const canonicalId = canonicalByAlias.get(aliasId)!;
      try {
        await this.mergeChatInto(sessionId, aliasId, canonicalId);
      } catch (e) {
        this.logger.warn(
          `[${sessionId}] no se pudo fusionar ${aliasId} en ${canonicalId}: ${e.message}`,
        );
      }
    }
  }

  private async mergeChatInto(
    sessionId: string,
    aliasId: string,
    canonicalId: string,
  ) {
    const moved = await this.messageRepo.manager.transaction(async (em) => {
      const msgRepo = em.getRepository(WhatsappMessage);
      const aliasMsgs = await msgRepo.find({
        where: { sessionId, chatId: aliasId },
        select: ['messageId'],
      });

      // Si la BD permite el mismo messageId en ambos chats, se descartan los
      // repetidos del alias en vez de chocar con el índice único.
      for (let i = 0; i < aliasMsgs.length; i += 500) {
        const ids = aliasMsgs.slice(i, i + 500).map((m) => m.messageId);
        const dupes = await msgRepo.find({
          where: { sessionId, chatId: canonicalId, messageId: In(ids) },
          select: ['messageId'],
        });
        if (dupes.length) {
          await msgRepo.delete({
            sessionId,
            chatId: aliasId,
            messageId: In(dupes.map((d) => d.messageId)),
          });
        }
      }
      await msgRepo.update(
        { sessionId, chatId: aliasId },
        { chatId: canonicalId },
      );
      await em
        .getRepository(WhatsappChat)
        .delete({ sessionId, chatId: aliasId });
      return aliasMsgs.length;
    });
    this.logger.log(
      `[${sessionId}] chat duplicado ${aliasId} fusionado en ${canonicalId} (${moved} mensajes revisados)`,
    );
  }

  async syncMessagesForChat(sessionId: string, chatId: string, limit = 50) {
    let messages;
    try {
      messages = await this.commandsService.getChatMessages(
        sessionId,
        chatId,
        limit,
      );
    } catch (e) {
      // Mismo comportamiento que antes cuando `getClient` devolvía null:
      // si no está conectado, simplemente no hay nada nuevo que insertar.
      this.logger.debug(
        `No se pudieron traer mensajes de ${chatId} (${sessionId}): ${e.message}`,
      );
      return { chatId, newCount: 0 };
    }

    const rows = messages.map((m) => ({
      sessionId,
      chatId,
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
      serializedId: m.serializedId,
      mentionsMe: m.mentionsMe,
    }));

    const result = await this.messageRepo
      .createQueryBuilder()
      .insert()
      .into(this.messageRepo.target)
      .values(rows)
      .orIgnore()
      .execute();

    return { chatId, newCount: result.identifiers.filter(Boolean).length };
  }

  /**
   * Suma de no leídos de chats individuales + grupos de esta conexión.
   * Antes solo se sumaba whatsapp_chat, así que un mensaje de grupo nunca
   * se reflejaba en el total emitido por socket.
   */
  private async getUnreadTotal(
    sessionId: string,
    whatsappConnectionId: string | null,
  ): Promise<number> {
    const { total: chatsTotal } = await this.chatRepo
      .createQueryBuilder('c')
      .select('COALESCE(SUM(c.unreadCount), 0)', 'total')
      .where('c.sessionId = :sessionId', { sessionId })
      .getRawOne();

    let groupsTotal = 0;
    if (whatsappConnectionId) {
      const raw = await this.groupRepo
        .createQueryBuilder('g')
        .select('COALESCE(SUM(g.unreadCount), 0)', 'total')
        .where('g.whatsappConnectionId = :whatsappConnectionId', {
          whatsappConnectionId,
        })
        .getRawOne();
      groupsTotal = Number(raw.total);
    }

    return Number(chatsTotal) + groupsTotal;
  }
}
