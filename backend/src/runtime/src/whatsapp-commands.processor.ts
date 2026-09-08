import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import {
  WHATSAPP_COMMANDS_QUEUE,
  WhatsappCommand,
  WhatsappCommandResult,
  WhatsappCommandType,
} from './../../shared/whatsapp-contracts';
import { WhatsappRuntimeService } from './whatsapp-runtime.service';

/**
 * Consume `whatsapp-commands` y ejecuta cada orden contra la sesión real de
 * whatsapp-web.js. Es el reemplazo de las llamadas directas que hoy hace
 * `WhatsappController` sobre `WHATSAPP_PROVIDER` — la diferencia es que esas
 * llamadas ahora llegan por cola, desde otro proceso.
 *
 * DEPENDE DE: `whatsapp-runtime.service.ts` (aún no construido — es el
 * siguiente archivo). Para que este processor compile y funcione, esa clase
 * debe exponer exactamente estos métodos:
 *
 *   connect(connectionId: string): Promise<void>
 *   getStatus(connectionId: string): WhatsappConnectionStatus   (síncrono, lee el Map en memoria)
 *   logout(connectionId: string): Promise<void>
 *   sendText(connectionId, chatId, text): Promise<SendResultInterface>
 *   sendImages(connectionId, groupId, imageUrls, caption?): Promise<SendResultInterface>
 *   sendMedia(connectionId, chatId, media, options?): Promise<SendResultInterface>
 *   getMedia(connectionId, messageId): Promise<WhatsappMediaPayload | null>
 *   getContact(connectionId, chatId): Promise<WhatsappContact>
 *   getAllChats(connectionId): Promise<WhatsappChatSummary[]>
 *
 * Son básicamente los mismos métodos que ya existen hoy en
 * `whatsapp-web.provider.ts` — al moverlo, casi no cambia la firma.
 *
 * Nota sobre reintentos: este processor NO decide cuántas veces se reintenta
 * un comando fallido — eso lo configura quien lo encola
 * (`whatsapp-commands.queue.ts`), porque reintentar un `send-text` sin
 * cuidado puede duplicar un mensaje ya enviado. Aquí solo logueamos y
 * relanzamos el error para que BullMQ decida según esa configuración.
 */
@Injectable()
@Processor(WHATSAPP_COMMANDS_QUEUE, { concurrency: 20 })
export class WhatsappCommandsProcessor extends WorkerHost {
  private readonly logger = new Logger(WhatsappCommandsProcessor.name);

  constructor(private readonly runtime: WhatsappRuntimeService) {
    super();
  }

  async process(
    job: Job<WhatsappCommand>,
  ): Promise<WhatsappCommandResult<WhatsappCommandType>> {
    const command = job.data;
    this.logger.debug(
      `[${command.connectionId}] ejecutando comando: ${command.type}`,
    );

    try {
      return await this.execute(command);
    } catch (err) {
      this.logger.error(
        `[${command.connectionId}] falló el comando ${command.type}: ${err.message}`,
      );
      throw err; // deja que BullMQ reintente o marque como failed, según la config del job
    }
  }

  private async execute(
    command: WhatsappCommand,
  ): Promise<WhatsappCommandResult<WhatsappCommandType>> {
    switch (command.type) {
      case 'connect':
        await this.runtime.connect(command.connectionId);
        return { status: this.runtime.getStatus(command.connectionId) };

      case 'logout':
        await this.runtime.logout(command.connectionId);
        return { ok: true };

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

      default: {
        // Si TypeScript se queja aquí de que `command` no es `never`, es la
        // señal de que se añadió un tipo nuevo a `WhatsappCommand` en
        // shared/whatsapp-contracts/commands.ts sin agregar su caso arriba.
        const exhaustiveCheck: never = command;
        throw new Error(
          `Comando desconocido: ${(exhaustiveCheck as WhatsappCommand).type}`,
        );
      }
    }
  }
}
