import {
  SendResultInterface,
  WhatsappChatSummary,
  WhatsappConnectionStatus,
  WhatsappContact,
  WhatsappMediaPayload,
  WhatsappRawMessage,
} from './types';

/**
 * Nombre de la cola BullMQ por la que la API envía órdenes al runtime.
 * Se usa tanto en `whatsapp-commands.queue.ts` (API, productor) como en
 * `whatsapp-commands.processor.ts` (runtime, consumidor).
 */
export const WHATSAPP_COMMANDS_QUEUE = 'whatsapp-commands';

/**
 * Cada variante corresponde 1:1 a un método que hoy expone `WhatsappController`.
 * Sustituye a las llamadas directas `this.provider.xxx()` de la Fase 0.
 *
 * Nota de nombres: en el código actual conviven `sessionId` y `connectionId`
 * para el mismo valor (según el archivo). Aquí se estandariza a `connectionId`
 * en todos los comandos y eventos — al tocar el runtime habrá que renombrar
 * los `sessionId` de los `emit(...)` para que coincida.
 */
export type WhatsappCommand =
  | { type: 'connect'; connectionId: string }
  | { type: 'logout'; connectionId: string }
  | { type: 'send-text'; connectionId: string; chatId: string; text: string }
  | {
      type: 'send-images';
      connectionId: string;
      groupId: string;
      imageUrls: string[];
      caption?: string;
    }
  | {
      type: 'send-media';
      connectionId: string;
      chatId: string;
      media: WhatsappMediaPayload;
      options?: { caption?: string; sendAudioAsVoice?: boolean };
    }
  | { type: 'get-media'; connectionId: string; messageId: string }
  | { type: 'get-contact'; connectionId: string; chatId: string }
  // Reemplaza tanto a `provider.getAllChats` como al endpoint eliminado
  // `GET update-groups` (que llamaba `provider.getGroups` directo).
  | { type: 'sync-all'; connectionId: string }
  // Reemplaza el `getClient(sessionId)` + `chat.fetchMessages()` que hacía
  // `WhatsappSyncService.syncMessagesForChat` directo sobre el Client vivo.
  | {
      type: 'get-chat-messages';
      connectionId: string;
      chatId: string;
      limit?: number;
    }
  // Elimina el chat del WhatsApp real (como "Eliminar chat" en la app) —
  // no borra mensajes del otro lado, solo de tu propia vista, igual que
  // hace la app oficial.
  | { type: 'delete-chat'; connectionId: string; chatId: string };

export type WhatsappCommandType = WhatsappCommand['type'];

/**
 * Qué devuelve `process()` en el runtime para cada tipo de comando.
 * Sirve para tipar el resultado de `job.waitUntilFinished(...)` en
 * `whatsapp-commands.service.ts` sin castear a `any`.
 */
export interface WhatsappCommandResultMap {
  connect: { status: WhatsappConnectionStatus };
  logout: { ok: true };
  'send-text': SendResultInterface;
  'send-images': SendResultInterface;
  'send-media': SendResultInterface;
  'get-media': WhatsappMediaPayload | null;
  'get-contact': WhatsappContact;
  'sync-all': WhatsappChatSummary[];
  'get-chat-messages': WhatsappRawMessage[];
  'delete-chat': { ok: boolean; error?: string };
}

export type WhatsappCommandResult<T extends WhatsappCommandType> =
  WhatsappCommandResultMap[T];

/** Nombre del job BullMQ — todos los comandos van bajo el mismo nombre de job. */
export const WHATSAPP_COMMAND_JOB_NAME = 'whatsapp-command';
