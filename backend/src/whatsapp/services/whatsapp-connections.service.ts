import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UsersService } from 'src/users/users.service';
import { WhatsappConnections } from 'src/database/entities/whatsapp-conections.entity';
import { WhatsappConnectionStatus } from 'shared/whatsapp-contracts';

@Injectable()
export class WhatsappConnectionsService {
  constructor(
    @InjectRepository(WhatsappConnections)
    private repo: Repository<WhatsappConnections>,
    private readonly usersService: UsersService,
  ) {}

  async create(data: {
    fullName: string;
    userId: string;
    nameUserConnected: string;
  }) {
    const connections = await this.findAllByUserID(data.userId);

    let connectionId: string;
    let existingConnection;

    do {
      connectionId = Math.floor(
        100000000 + Math.random() * 900000000,
      ).toString();

      existingConnection = await this.findByConnectionId(connectionId);
    } while (existingConnection);

    const connection = this.repo.create({
      userId: data.userId,
      nameUserConnected:
        connections.length > 0 ? data.nameUserConnected : data.fullName,
      connectionId,
    });

    return this.repo.save(connection);
  }

  async belongsToUser(connectionId: string, userId: string): Promise<boolean> {
    const connection = await this.repo.findOne({
      where: { connectionId: connectionId, userId },
    });
    return !!connection;
  }

  async findAllByUserID(userId: string) {
    return await this.repo.find({
      where: { userId },
      relations: ['whatsappGroups'],
    });
  }

  async findAllByUser(userId: string) {
    return this.repo.find({ where: { userId } });
  }

  async findByConnectionId(
    connectionId: string,
  ): Promise<WhatsappConnections | null> {
    return this.repo.findOne({
      where: { connectionId },
    });
  }

  async updateStatus(
    connectionId: string,
    status: WhatsappConnectionStatus,
  ): Promise<void> {
    await this.repo.update({ connectionId }, { status });
  }
}
