import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { bullConfig } from '../../src/config/bullmq.config';
import {
  WHATSAPP_COMMANDS_QUEUE,
  WHATSAPP_PERSIST_EVENTS_QUEUE,
} from '../../shared/whatsapp-contracts';
import { WhatsappRuntimeService } from './whatsapp-runtime.service';
import { WhatsappCommandsProcessor } from './whatsapp-commands.processor';
// ── Baileys reemplaza a whatsapp-web.js: WhatsappRemoteAuthStore
// (RemoteAuth de wweb.js) → WhatsappBaileysAuthStore; y el Client de
// wweb.js que antes vivía dentro de WhatsappRuntimeService ahora es
// WhatsappBaileysProvider. Ver los comentarios de cabecera de cada uno.
import { WhatsappBaileysProvider } from './whatsapp-baileys.provider';
import { WhatsappBaileysAuthStore } from './whatsapp-baileys-auth.store';

@Module({
  imports: [
    // Este proceso también lee el .env (mismas variables DATABASE_*/REDIS_*
    // que la API) — sin esto, process.env quedaría vacío al arrancar solo.
    ConfigModule.forRoot({ isGlobal: true }),
    BullModule.forRoot(bullConfig),
    BullModule.registerQueue(
      // el WhatsappCommandsProcessor la consume
      { name: WHATSAPP_COMMANDS_QUEUE },
      // el runtime solo produce acá — la consume WhatsappPersistEventsProcessor del lado API
      { name: WHATSAPP_PERSIST_EVENTS_QUEUE },
    ),
  ],
  providers: [
    WhatsappRuntimeService,
    WhatsappCommandsProcessor,
    WhatsappBaileysAuthStore,
    WhatsappBaileysProvider,
  ],
})
export class RuntimeModule {}
