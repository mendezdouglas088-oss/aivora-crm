import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { Repository } from 'typeorm';

@Injectable()
export class WhatsappChatService {
  constructor(
    @InjectRepository(WhatsappChat)
    private readonly chatRepo: Repository<WhatsappChat>,
  ) {}

  async findAll(sessionId: string): Promise<WhatsappChat[]> {
    return this.chatRepo.find({
      where: { sessionId },
      order: { lastMessageAt: { direction: 'DESC', nulls: 'LAST' } },
    });
  }

  async updateReadCount(
    sessionId: string,
    chatId: string,
    unreadCount: number,
  ) {
    await this.chatRepo.update({ sessionId, chatId }, { unreadCount });
  }

  async getUnreadTotal(sessionId: string) {
    const { total } = await this.chatRepo
      .createQueryBuilder('c')
      .select('COALESCE(SUM(c.unreadCount), 0)', 'total')
      .where('c.sessionId = :sessionId', { sessionId })
      .getRawOne();
    return Number(total);
  }

  async findNew(sessionId: string): Promise<WhatsappChat[]> {
    return this.chatRepo.find({
      where: { sessionId, isNew: true },
      order: { lastMessageAt: { direction: 'DESC', nulls: 'LAST' } },
    });
  }

  async markSeen(sessionId: string, chatId: string) {
    await this.chatRepo.update({ sessionId, chatId }, { isNew: false });
  }

  async findUnregistered(sessionId: string): Promise<WhatsappChat[]> {
    return this.chatRepo.find({
      where: { sessionId, isSavedContact: false },
      order: { lastMessageAt: { direction: 'DESC', nulls: 'LAST' } },
    });
  }

  async deleteChat(sessionId: string, chatId: string): Promise<void> {
    await this.chatRepo.delete({ sessionId, chatId });
  }
}
