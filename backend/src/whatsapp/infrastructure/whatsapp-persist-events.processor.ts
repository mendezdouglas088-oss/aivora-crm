import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Job } from 'bullmq';
import { Repository } from 'typeorm';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { RealtimeGateway } from 'src/realtime/realtime.gateway';
import {
  WHATSAPP_PERSIST_EVENTS_QUEUE,
  WhatsappPersistEventJob,
} from 'shared/whatsapp-contracts';
import { WhatsappConnectionsService } from '../services/whatsapp-connections.service';

/**
 * Mismo cuerpo que tenía `@OnEvent('whatsapp.message.persist')` en
 * whatsapp-events.listener.ts — solo que ahora consume una cola BullMQ
 * (la produce whatsapp-runtime.service.ts desde el otro proceso) en vez de
 * escuchar un emit local. Por eso ahora sí importan los reintentos: si
 * falla el guardado, BullMQ lo reintenta según la config del job
 * (definida en whatsapp-runtime.service.ts al encolar).
 *
 * Único cambio real de contenido: `payload.sessionId` → `payload.connectionId`,
 * porque el contrato compartido estandarizó ese nombre.
 *
 * --- Fixes de la ronda de revisión (items 2, 3, 4 y 5) ---
 *  (4) Ya no se pisa `lastMessageAt`/`lastMessage` a ciegas: si lo que ya
 *      hay guardado es más nuevo que este evento (puede pasar si llega
 *      fuera de orden respecto al sync periódico), se conserva lo viejo.
 *  (3) Se detecta si el chat/grupo YA existía antes del upsert. Si es
 *      nuevo, se emite `emitNewChat`/`emitNewGroup` de inmediato en vez de
 *      esperar al próximo sync periódico (hasta 3 min de retraso antes).
 *      De paso, se completa `whatsappConnectionId` al crear un grupo desde
 *      un evento en vivo — antes quedaba en null hasta que corría el sync,
 *      y `findAllById(userId, connectionId)` no lo encontraba mientras tanto.
 *  (2) Se agrega `emitChatUpdated`, que dispara SIEMPRE que se inserta un
 *      mensaje nuevo (propio o ajeno). Antes `emitNewMessages` solo se
 *      disparaba para mensajes ajenos, así que enviar un mensaje (desde la
 *      API o desde el celular vinculado) no se reflejaba en tiempo real en
 *      otras pestañas/dispositivos conectados — solo se enteraban en el
 *      siguiente sync. `emitNewMessages` se conserva tal cual para el
 *      badge de no leídos, que sigue sin deber contar mensajes propios.
 *  (5) El total de no leídos que se emite ahora suma chats + grupos de la
 *      conexión (getUnreadTotal). Antes solo sumaba whatsapp_chat, así que
 *      un mensaje de grupo nunca se reflejaba en el total.
 *
 * OJO: `emitChatUpdated` es un método NUEVO que hay que agregar a
 * `RealtimeGateway` (src/realtime/realtime.gateway.ts) — ese archivo no
 * estaba en lo que se subió para revisión, así que no se pudo tocar acá.
 * Ver el mensaje de la conversación para el snippet sugerido.
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
  ) {
    super();
  }

  async process(job: Job<WhatsappPersistEventJob>) {
    const { payload } = job.data;

    try {
      const result = await this.messageRepo
        .createQueryBuilder()
        .insert()
        .values({
          sessionId: payload.connectionId,
          chatId: payload.chatId,
          messageId: payload.messageId,
          fromMe: payload.fromMe,
          body: payload.body,
          timestamp: payload.timestamp,
          isRead: payload.fromMe,
          ack: payload.ack,
          isGroup: payload.isGroup,
          type: payload.type,
          hasMedia: payload.hasMedia,
          author: payload.author,
          authorName: payload.authorName,
          mentionsMe: payload.mentionsMe,
          serializedId: payload.serializedId,
        })
        .orIgnore()
        .execute();

      const isNewMessageRow = result.identifiers.length > 0;
      let isNewConversation = false;

      if (payload.isGroup) {
        // Vemos si el grupo ya existía ANTES de tocarlo: sirve tanto para
        // no pisar un lastMessageAt más nuevo (fix 4) como para saber si
        // hay que avisar por socket de un grupo nuevo (fix 3).
        const existingGroup = await this.groupRepo.findOne({
          where: { whatsappGroupId: payload.chatId },
          select: ['lastMessageAt', 'lastMessage'],
        });
        isNewConversation = !existingGroup;

        const isStale =
          !!existingGroup?.lastMessageAt &&
          existingGroup.lastMessageAt > payload.timestamp;

        // Antes no se seteaba whatsappConnectionId acá: un grupo creado
        // por primera vez desde un evento en vivo quedaba con esa columna
        // en null y no aparecía en /whatsapp/groups (filtra por conexión)
        // hasta que corría el sync periódico.
        const connection = await this.whatsappConnectionsService
          .findByConnectionId(payload.connectionId)
          .catch(() => null);

        await this.groupRepo.upsert(
          [
            {
              whatsappGroupId: payload.chatId,
              title: payload.chatName,
              lastMessage: isStale ? existingGroup!.lastMessage : payload.body,
              lastMessageAt: isStale
                ? existingGroup!.lastMessageAt
                : payload.timestamp,
              unreadCount: payload.unreadCount,
              whatsappConnectionId: connection?.id ?? null,
            },
          ],
          ['whatsappGroupId'],
        );

        if (isNewConversation) {
          this.gateway.emitNewGroup(payload.connectionId, {
            whatsappGroupId: payload.chatId,
            title: payload.chatName,
            unreadCount: payload.unreadCount,
          });
        }
      } else {
        // no pisar isNew: si el chat ya existía, se conserva su valor actual
        const existingChat = await this.chatRepo.findOne({
          where: { sessionId: payload.connectionId, chatId: payload.chatId },
          select: ['isNew', 'lastMessageAt', 'lastMessage'],
        });
        isNewConversation = !existingChat;

        const isStale =
          !!existingChat?.lastMessageAt &&
          existingChat.lastMessageAt > payload.timestamp;

        await this.chatRepo.upsert(
          [
            {
              sessionId: payload.connectionId,
              chatId: payload.chatId,
              name: payload.chatName,
              lastMessage: isStale ? existingChat!.lastMessage : payload.body,
              lastMessageAt: isStale
                ? existingChat!.lastMessageAt
                : payload.timestamp,
              unreadCount: payload.unreadCount,
              isNew: existingChat ? existingChat.isNew : true,
            },
          ],
          ['sessionId', 'chatId'],
        );

        if (isNewConversation) {
          this.gateway.emitNewChat(payload.connectionId, {
            chatId: payload.chatId,
            name: payload.chatName,
            unreadCount: payload.unreadCount,
          });
        }
      }

      if (isNewMessageRow) {
        // Dispara SIEMPRE (propio o ajeno) para que cualquier
        // pestaña/dispositivo conectado reordene la lista y refresque el
        // preview del último mensaje.
        this.gateway.emitChatUpdated(payload.connectionId, {
          chatId: payload.chatId,
          isGroup: payload.isGroup,
          name: payload.chatName,
          lastMessage: payload.body,
          lastMessageAt: payload.timestamp,
          unreadCount: payload.unreadCount,
          fromMe: payload.fromMe,
        });

        if (!payload.fromMe) {
          // Este sí sigue siendo específico de "mensajes nuevos sin leer";
          // no debe dispararse con mensajes propios.
          const total = await this.getUnreadTotal(payload.connectionId);
          this.gateway.emitNewMessages(
            payload.connectionId,
            payload.chatId,
            1,
            total,
          );
        }
      }
    } catch (err) {
      this.logger.error(
        `Falló al persistir mensaje ${payload.messageId} de ${payload.connectionId}: ${err.message}`,
      );
      throw err; // deja que BullMQ reintente según la config del job
    }
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
