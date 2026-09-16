import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
  OnGatewayConnection,
} from '@nestjs/websockets';
import { OnEvent } from '@nestjs/event-emitter';
import { Server, Socket } from 'socket.io';
import { JwtService } from '@nestjs/jwt';
import { WhatsappConnectionsService } from 'src/whatsapp/services/whatsapp-connections.service';

@WebSocketGateway({ cors: { origin: process.env.FRONTEND_DOMAIN } })
export class RealtimeGateway implements OnGatewayConnection {
  @WebSocketServer() server: Server;

  constructor(
    private readonly jwtService: JwtService,
    private readonly connectionsService: WhatsappConnectionsService,
  ) {}

  async handleConnection(client: Socket) {
    try {
      const token = client.handshake.auth?.token;
      const payload = await this.jwtService.verifyAsync(token);
      client.data.userId = payload.id;
    } catch {
      client.disconnect();
    }
  }
  emitNewMessages(
    sessionId: string,
    chatId: string,
    newCount: number,
    unreadTotal: number,
  ) {
    this.server
      .to(sessionId)
      .emit('whatsapp:new-messages', { sessionId, chatId, newCount });
    this.server
      .to(sessionId)
      .emit('whatsapp:unread-total', { sessionId, total: unreadTotal });
  }

  emitChatUpdated(
    connectionId: string,
    payload: {
      chatId: string;
      isGroup: boolean;
      name: string;
      lastMessage: string;
      lastMessageAt: number;
      unreadCount: number;
      fromMe: boolean;
    },
  ) {
    this.server.to(connectionId).emit('whatsapp:chat-updated', payload);
  }

  emitNewChat(
    sessionId: string,
    chat: { chatId: string; name: string; unreadCount: number },
  ) {
    this.server.to(sessionId).emit('whatsapp:new-chat', { sessionId, ...chat });
  }

  emitNewGroup(
    sessionId: string,
    group: { whatsappGroupId: string; title: string; unreadCount: number },
  ) {
    this.server
      .to(sessionId)
      .emit('whatsapp:new-group', { sessionId, ...group });
  }

  @SubscribeMessage('join')
  async onJoin(
    @MessageBody() connectionId: string,
    @ConnectedSocket() client: Socket,
  ) {
    const owns = await this.connectionsService.belongsToUser(
      connectionId,
      client.data.userId,
    );
    if (!owns) return;
    client.join(connectionId);
  }

  @SubscribeMessage('leave')
  onLeave(
    @MessageBody() connectionId: string,
    @ConnectedSocket() client: Socket,
  ) {
    client.leave(connectionId);
  }

  @OnEvent('whatsapp.call')
  handleCall(payload: {
    sessionId: string;
    from: string;
    isVideo: boolean;
    isGroup: boolean;
    timestamp: number;
  }) {
    this.server.to(payload.sessionId).emit('whatsapp:call', payload);
  }

  @OnEvent('whatsapp.message.received')
  handleWhatsappMessage(payload: any) {
    this.server.to(payload.sessionId).emit('whatsapp:message', payload);
  }
  @OnEvent('whatsapp.qr')
  handleQr(payload: { connectionId: string; qr: string }) {
    this.server.to(payload.connectionId).emit('whatsapp:qr', payload);
  }

  @OnEvent('whatsapp.status')
  handleStatus(payload: { connectionId: string; status: string }) {
    this.server.to(payload.connectionId).emit('whatsapp:status', payload);
  }
}
