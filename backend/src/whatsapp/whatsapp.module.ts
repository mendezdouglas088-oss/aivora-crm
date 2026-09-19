import { forwardRef, Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';

import { WhatsappController } from './controllers/whatsapp.controller';
import { WhatsappGroup } from 'src/database/entities/whatsapp-group.entity';
import { ConfigModule } from 'src/config/config.module';
import { UsersModule } from 'src/users/users.module';
import { AuthModule } from 'src/auth/auth.module';
import { WhatsappGroupService } from './services/whatsapp-group.service';
import { WhatsappConnectionsService } from './services/whatsapp-connections.service';
import { WhatsappConnectionsController } from './controllers/whatsapp-connections.controller';
import { WhatsappConnections } from 'src/database/entities/whatsapp-conections.entity';
import { WhatsappSyncService } from './application/whatsapp-sync.service';
import { WhatsappSyncProcessor } from './infrastructure/producer/whatsapp-sync.processor';
import { WhatsappSyncQueue } from './infrastructure/jobs/whatsapp-sync.queue';
import { WhatsappEventsListener } from './infrastructure/whatsapp-events.listener';
import { RealtimeModule } from 'src/realtime/realtime.module';
import { WhatsappChat } from 'src/database/entities/whatsapp-chat.entity';
import { WhatsappMessage } from 'src/database/entities/whatsapp-message.entity';
import { WhatsappChatService } from './services/whatsapp-chat.service';
import { WhatsappMessageService } from './services/whatsapp-message.service';
import { WhatsappRegisteredContact } from 'src/database/entities/whatsapp-registered-contact.entity';
import { WhatsappRegisteredContactsService } from './services/whatsapp-registered-contact.service';
import { WhatsappRegisteredContactsController } from './controllers/whatsapp-registered-contacts.controller';

// ── nuevo en la Fase 1 ──────────────────────────────────────────────
import {
  WHATSAPP_COMMANDS_QUEUE,
  WHATSAPP_PERSIST_EVENTS_QUEUE,
} from 'shared/whatsapp-contracts';
import { WhatsappCommandsQueue } from './infrastructure/jobs/whatsapp-commands.queue';
import { WhatsappCommandsService } from './services/whatsapp-commands.service';
import { WhatsappPersistEventsProcessor } from './infrastructure/whatsapp-persist-events.processor';
import { WhatsappLiveEventsBridge } from './infrastructure/whatsapp-live-events.bridge';

@Module({
  imports: [
    BullModule.registerQueue(
      { name: 'whatsapp-sync' },
      { name: WHATSAPP_COMMANDS_QUEUE }, // WhatsappCommandsQueue la usa como productor
      { name: WHATSAPP_PERSIST_EVENTS_QUEUE }, // WhatsappPersistEventsProcessor la consume
    ),
    HttpModule,
    TypeOrmModule.forFeature([
      WhatsappGroup,
      WhatsappConnections,
      WhatsappChat,
      WhatsappRegisteredContact,
      WhatsappMessage,
    ]),
    ConfigModule,
    AuthModule,
    UsersModule,
    forwardRef(() => RealtimeModule),
  ],
  providers: [
    WhatsappGroupService,
    WhatsappConnectionsService,
    WhatsappEventsListener,
    WhatsappSyncService,
    WhatsappSyncProcessor,
    WhatsappSyncQueue,
    WhatsappChatService,
    WhatsappMessageService,
    WhatsappRegisteredContactsService,
    // reemplazan a WhatsappScheduler + { provide: WHATSAPP_PROVIDER, ... }
    WhatsappCommandsQueue,
    WhatsappCommandsService,
    WhatsappPersistEventsProcessor,
    WhatsappLiveEventsBridge,
  ],
  exports: [
    WhatsappGroupService,
    WhatsappConnectionsService,
    WhatsappSyncService,
    WhatsappSyncProcessor,
    WhatsappSyncQueue,
    WhatsappChatService,
    WhatsappMessageService,
    WhatsappRegisteredContactsService,
    // reemplaza el WHATSAPP_PROVIDER exportado antes (lo usa publication.scheduler.ts)
    WhatsappCommandsService,
  ],
  controllers: [
    WhatsappController,
    WhatsappConnectionsController,
    WhatsappRegisteredContactsController,
  ],
})
export class WhatsappModule {}
