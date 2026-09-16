import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { In, Repository } from 'typeorm';

@Injectable()
export class WhatsappMessageService {
  constructor(
    @InjectRepository(WhatsappMessage)
    private readonly messageRepo: Repository<WhatsappMessage>,
    @InjectRepository(WhatsappGroup)
    private readonly groupRepo: Repository<WhatsappGroup>,
  ) {}

  async findAll(
    sessionId: string,
    chatId: string,
    limit: number,
  ): Promise<WhatsappMessage[]> {
    const messages = this.messageRepo.find({
      where: { sessionId, chatId },
      order: { timestamp: 'DESC' },
      take: limit,
    });
    return (await messages).reverse();
  }

  async markAsRead(sessionId: string, chatId: string) {
    await this.messageRepo.update(
      { sessionId, chatId, isRead: false },
      { isRead: true },
    );
  }

  async findMentions(sessionId: string, limit: number) {
    const messages = await this.messageRepo.find({
      where: { sessionId, mentionsMe: true },
      order: { timestamp: 'DESC' },
      take: limit,
    });
    if (!messages.length) return [];

    const groupIds = [...new Set(messages.map((m) => m.chatId))];
    const groups = await this.groupRepo.find({
      where: { whatsappGroupId: In(groupIds) },
      select: ['whatsappGroupId', 'title'],
    });
    const titleByGroupId = new Map(
      groups.map((g) => [g.whatsappGroupId, g.title]),
    );

    return messages.map((m) => ({
      ...m,
      groupTitle: titleByGroupId.get(m.chatId) ?? null,
    }));
  }

  async deleteAllForChat(sessionId: string, chatId: string): Promise<void> {
    await this.messageRepo.delete({ sessionId, chatId });
  }
}
