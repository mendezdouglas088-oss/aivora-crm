import {
  Controller,
  Get,
  Post,
  Delete,
  Body,
  Query,
  Res,
  UseGuards,
  Req,
  DefaultValuePipe,
  ParseIntPipe,
  Param,
  HttpStatus,
} from '@nestjs/common';
import { Response } from 'express';
import { JwtAuthGuard } from 'src/auth/guards/jwt-auth.guard';
import { WhatsappGroupService } from '../services/whatsapp-group.service';
import { WhatsappChatService } from '../services/whatsapp-chat.service';
import { WhatsappMessageService } from '../services/whatsapp-message.service';
import { RealtimeGateway } from 'src/realtime/realtime.gateway';
import { WhatsappSyncQueue } from '../infrastructure/jobs/whatsapp-sync.queue';
import { WhatsappCommandsService } from '../services/whatsapp-commands.service';

@UseGuards(JwtAuthGuard)
@Controller('whatsapp')
export class WhatsappController {
  constructor(
    private readonly commands: WhatsappCommandsService,
    private readonly whatsappGroupService: WhatsappGroupService,
    private readonly chatsService: WhatsappChatService,
    private readonly messageService: WhatsappMessageService,
    private readonly gateway: RealtimeGateway,
    private readonly syncQueue: WhatsappSyncQueue,
  ) {}

  @Post('connect')
  async connect(@Query('connectionId') connectionId: string) {
    return this.commands.connect(connectionId); // ya devuelve { status }
  }

  @Get('qr')
  async getQr(
    @Query('connectionId') connectionId: string,
    @Res() res: Response,
  ) {
    const status = await this.commands.getStatus(connectionId);

    // 'connecting' se agrega como red de seguridad extra: connect() ya es
    // idempotente (no hace nada si hay una sesión real en curso en este
    // mismo proceso), así que no duplica trabajo, pero cubre el caso de un
    // 'connecting' que quedó colgado sin más eventos.
    if (
      ['disconnected', 'error', 'auth_failed', 'connecting'].includes(status)
    ) {
      await this.commands.connect(connectionId); // crea la sesión si no existe
    }

    const qr = await this.commands.getQr(connectionId);

    // 'waiting_qr' pero sin QR en Redis = estado huérfano (runtime caído o
    // reiniciado). Antes nadie hacía nada y el front recibía 202 para siempre.
    if (!qr && status === 'waiting_qr') {
      await this.commands.connect(connectionId);
    }

    if (!qr) {
      res
        .status(202)
        .json({ message: 'Generando QR, reintenta en unos segundos' });
      return;
    }
    res.type('image/png').send(qr);
  }

  @Get('chats')
  async getChats(@Query('connectionId') connectionId: string) {
    const chats = await this.chatsService.findAll(connectionId);
    this.syncQueue.enqueueSync(connectionId).catch(() => {}); // refresco en background, no bloquea la respuesta
    return chats;
  }

  @Get('messages')
  async getMessages(
    @Query('connectionId') connectionId: string,
    @Query('chatId') chatId: string,
    @Query('limit', new DefaultValuePipe(150), ParseIntPipe) limit: number,
  ) {
    return await this.messageService.findAll(connectionId, chatId, limit);
  }

  @Post('mark-as-read')
  async markChatAsRead(
    @Query('connectionId') connectionId: string,
    @Query('chatId') chatId: string,
  ) {
    await this.messageService.markAsRead(connectionId, chatId);
    await this.chatsService.updateReadCount(connectionId, chatId, 0);
    const totalUnread = await this.chatsService.getUnreadTotal(connectionId);
    this.gateway.emitNewMessages(connectionId, chatId, 0, totalUnread);
  }

  @Post('logout')
  async logout(@Query('connectionId') connectionId: string) {
    await this.commands.logout(connectionId);
    return { status: 'disconnected' };
  }

  @Get('status')
  async getStatus(@Query('connectionId') connectionId: string) {
    const status = await this.commands.getStatus(connectionId ?? '');
    return { status };
  }

  @Post('sync')
  async syncAll(@Query('connectionId') connectionId: string) {
    await this.syncQueue.enqueueFullSync(connectionId);
    return { queued: true };
  }

  /**
   * ANTES: llamaba `provider.getGroups(connectionId)` directo sobre el
   * Client vivo. Eso ya no es posible desde la API (el Client vive en el
   * runtime). Se une al mismo camino que `/whatsapp/sync`: encola un
   * sync-all en background y devuelve el estado actual en BD — el frontend
   * se entera de los grupos nuevos por el evento de socket 'whatsapp:new-group'
   * (emitNewGroup, disparado desde whatsapp-sync.service.ts).
   *
   * CAMBIO DE COMPORTAMIENTO: antes la respuesta era síncrona y 100% fresca;
   * ahora es "dispara refresco + devuelve lo último que hay en BD".
   */
  @Get('update-groups')
  async updateGroups(
    @Req() req: any,
    @Query('connectionId') connectionId: string,
  ) {
    try {
      await this.syncQueue.enqueueFullSync(connectionId);
      return await this.whatsappGroupService.findAllById(
        req.user.id,
        connectionId,
      );
    } catch (error) {
      return {
        status: error.status || HttpStatus.INTERNAL_SERVER_ERROR,
        message: error.message,
      };
    }
  }

  @Get('groups')
  async getGroups(
    @Req() req: any,
    @Query('connectionId') connectionId?: string,
  ) {
    try {
      const user = req.user;
      this.syncQueue.enqueueSync(connectionId).catch(() => {}); // refresco en background, no bloquea la respuesta

      return await this.whatsappGroupService.findAllById(user.id, connectionId);
    } catch (error) {
      return {
        status: error.status || HttpStatus.INTERNAL_SERVER_ERROR,
        message: error.message,
      };
    }
  }

  @Post('send_message')
  async sendMessage(
    @Body() body: { connectionId: string; chatId: string; message: string },
  ) {
    return await this.commands.sendText(
      body.connectionId,
      body.chatId,
      body.message,
    );
  }

  @Post('send-image')
  async sendImage(
    @Body()
    body: {
      connectionId: string;
      groupId: string;
      imageUrls: string[];
      caption?: string;
    },
  ) {
    return await this.commands.sendImages(
      body.connectionId,
      body.groupId,
      body.imageUrls,
      body.caption,
    );
  }

  @Post('send-media')
  async sendMedia(
    @Body()
    body: {
      connectionId: string;
      chatId: string;
      mimetype: string;
      data: string; // base64
      filename?: string;
      caption?: string;
    },
  ) {
    return await this.commands.sendMedia(
      body.connectionId,
      body.chatId,
      { mimetype: body.mimetype, data: body.data, filename: body.filename },
      { caption: body.caption },
    );
  }

  @Get('media/:messageId')
  async getMedia(
    @Query('connectionId') connectionId: string,
    @Param('messageId') messageId: string,
  ) {
    return await this.commands.getMedia(connectionId, messageId);
  }

  @Get('chats/new')
  async getNewChats(@Query('connectionId') connectionId: string) {
    return this.chatsService.findNew(connectionId);
  }

  @Post('chats/:chatId/seen')
  async markChatSeen(
    @Query('connectionId') connectionId: string,
    @Param('chatId') chatId: string,
  ) {
    await this.chatsService.markSeen(connectionId, chatId);
    return { ok: true };
  }

  @Get('chats/unregistered')
  async getUnregisteredChats(@Query('connectionId') connectionId: string) {
    return this.chatsService.findUnregistered(connectionId);
  }

  /**
   * Borra el chat de WhatsApp real (como "Eliminar chat" en la app — no
   * borra mensajes del lado del otro contacto) y de la BD (chat + mensajes).
   * Si el runtime no está conectado o falla el borrado remoto, igual se
   * limpia la BD — se informa el error de WhatsApp en la respuesta para que
   * el frontend pueda avisar que puede requerir borrado manual.
   */
  @Delete('chats/:chatId')
  async deleteChat(
    @Query('connectionId') connectionId: string,
    @Param('chatId') chatId: string,
  ) {
    const whatsappResult = await this.commands
      .deleteChat(connectionId, chatId)
      .catch((e) => ({ ok: false, error: e.message }));

    await this.messageService.deleteAllForChat(connectionId, chatId);
    await this.chatsService.deleteChat(connectionId, chatId);

    return {
      removedFromDb: true,
      removedFromWhatsapp: whatsappResult.ok,
      whatsappError: whatsappResult.ok ? undefined : whatsappResult.error,
    };
  }

  @Get('groups/:groupId/messages')
  async getGroupMessages(
    @Query('connectionId') connectionId: string,
    @Param('groupId') groupId: string,
    @Query('limit', new DefaultValuePipe(150), ParseIntPipe) limit: number,
  ) {
    return this.messageService.findAll(connectionId, groupId, limit);
  }

  @Get('mentions')
  async getMentions(
    @Query('connectionId') connectionId: string,
    @Query('limit', new DefaultValuePipe(100), ParseIntPipe) limit: number,
  ) {
    return this.messageService.findMentions(connectionId, limit);
  }
}
