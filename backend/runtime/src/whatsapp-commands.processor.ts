import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import {
  WHATSAPP_COMMANDS_QUEUE,
  WhatsappCommand,
} from '../../shared/whatsapp-contracts';
import { WhatsappRuntimeService } from './whatsapp-runtime.service';

/**
 * Consumidor de la cola `whatsapp-commands` (la produce, del lado API,
 * `whatsapp-commands.queue.ts` vía `WhatsappCommandsService`). Por cada job
 * hace el switch por `command.type` y llama al método correspondiente de
 * `WhatsappRuntimeService` — lo que devuelve `process()` es justo lo que la
 * API recibe de vuelta en `job.waitUntilFinished(...)`.
 *
 * Nota: este archivo antes tenía, por error, una copia de los tipos de
 * `shared/whatsapp-contracts/commands.ts` en vez de la clase del processor
 * — por eso `runtime.module.ts` no encontraba `WhatsappCommandsProcessor`.
 */
@Injectable()
// Concurrencia > 1 (por defecto BullMQ procesa de a UN job): antes un sync-all
// lento dejaba en espera connect/send-text/etc. y la API los daba por
// caídos a los 30 s. Los comandos son independientes entre sí y
// WhatsappBaileysProvider.connect() ya es seguro ante llamadas simultáneas.
@Processor(WHATSAPP_COMMANDS_QUEUE, { concurrency: 5 })
export class WhatsappCommandsProcessor extends WorkerHost {
  private readonly logger = new Logger(WhatsappCommandsProcessor.name);

  constructor(private readonly runtime: WhatsappRuntimeService) {
    super();
  }

  async process(job: Job<WhatsappCommand>) {
    const command = job.data;

    switch (command.type) {
      case 'connect':
        await this.runtime.connect(command.connectionId);
        return { status: this.runtime.getStatus(command.connectionId) };

      case 'logout':
        await this.runtime.logout(command.connectionId);
        return { ok: true as const };

      case 'send-text':
        return this.runtime.sendText(
          command.connectionId,
          command.chatId,
          command.text,
        );

      case 'send-images':
        return this.runtime.sendImages(
          command.connectionId,
          command.groupId,
          command.imageUrls,
          command.caption,
        );

      case 'send-media':
        return this.runtime.sendMedia(
          command.connectionId,
          command.chatId,
          command.media,
          command.options,
        );

      case 'get-media':
        return this.runtime.getMedia(command.connectionId, command.messageId);

      case 'get-contact':
        return this.runtime.getContact(command.connectionId, command.chatId);

      case 'sync-all':
        return this.runtime.getAllChats(command.connectionId);

      case 'get-chat-messages':
        return this.runtime.getChatMessages(
          command.connectionId,
          command.chatId,
          command.limit,
        );

      case 'delete-chat':
        return this.runtime.deleteChat(command.connectionId, command.chatId);

      default: {
        const exhaustiveCheck: never = command;
        this.logger.warn(
          `Comando desconocido: ${JSON.stringify(exhaustiveCheck)}`,
        );
        throw new Error(
          `Comando desconocido: ${(command as WhatsappCommand).type}`,
        );
      }
    }
  }
}
