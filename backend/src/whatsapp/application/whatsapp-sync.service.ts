import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { RealtimeGateway } from 'src/realtime/realtime.gateway';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { WhatsappConnectionsService } from '../services/whatsapp-connections.service';
import { WhatsappCommandsService } from '../services/whatsapp-commands.service';

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

  async syncAll(sessionId: string) {
    // sessionId aquí ES connectionId — se mantiene el nombre por las
    // columnas de BD (WhatsappChat.sessionId), no por el contrato con el runtime.
    const allChats = await this.commandsService.syncAll(sessionId);

    const chatIds = allChats.filter((c) => !c.isGroup).map((c) => c.chatId);
    const groupIds = allChats.filter((c) => c.isGroup).map((c) => c.chatId);

    const existingChats = chatIds.length
      ? await this.chatRepo.find({
          where: { sessionId, chatId: In(chatIds) },
          select: ['chatId', 'isNew', 'lastMessageAt', 'lastMessage'],
        })
      : [];
    const existingChatsMap = new Map(existingChats.map((c) => [c.chatId, c]));

    const existingGroups = groupIds.length
      ? await this.groupRepo.find({
          where: { whatsappGroupId: In(groupIds) },
          select: ['whatsappGroupId', 'lastMessageAt', 'lastMessage'],
        })
      : [];
    const existingGroupsMap = new Map(
      existingGroups.map((g) => [g.whatsappGroupId, g]),
    );

    const connection =
      await this.whatsappConnectionsService.findByConnectionId(sessionId);
    const whatsappConnectionId = connection?.id ?? null;

    for (const c of allChats) {
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
            title: c.name,
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
            title: c.name,
            unreadCount: c.unreadCount,
          });
        }
      } else {
        await this.chatRepo.upsert(
          [
            {
              sessionId,
              chatId: c.chatId,
              name: c.name,
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
            name: c.name,
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
