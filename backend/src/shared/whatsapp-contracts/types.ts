/**
 * Tipos de datos puros, sin dependencia de `whatsapp-web.js`.
 *
 * Importante: este archivo NO puede importar nada de la librería `whatsapp-web.js`
 * (ni `Client`, ni `Message`, ni `MessageMedia`). Ese es justo el límite que separa
 * la API del runtime: si algo de aquí necesitara el tipo `Client`, sería señal de que
 * ese dato no debería cruzar el proceso.
 *
 * Viene de lo que antes vivía en:
 *   src/whatsapp/domain/whatsapp-provider.interface.ts
 * Se quitó de aquí: `WhatsappProvider` (se repartió entre commands.ts y events.ts)
 * y `WhatsappConnectionsInterface` (no se usaba en ningún lado).
 */

export type WhatsappMessageType =
  | 'chat'
  | 'image'
  | 'video'
  | 'audio'
  | 'ptt'
  | 'document'
  | 'sticker'
  | 'call_log'
  | 'location'
  | 'vcard'
  | 'unknown';

export type WhatsappConnectionStatus =
  | 'disconnected'
  | 'connecting'
  | 'waiting_qr'
  | 'connected'
  | 'auth_failed'
  | 'error';

export interface WhatsappMediaPayload {
  mimetype: string;
  data: string; // base64
  filename?: string;
}

export interface WhatsappMessagePersistPayload {
  connectionId: string;
  chatId: string;
  chatName: string;
  messageId: string;
  fromMe: boolean;
  body: string;
  timestamp: number;
  ack: number;
  unreadCount: number;
  isGroup: boolean;
  type: WhatsappMessageType;
  hasMedia: boolean;
  author?: string;
  authorName?: string;
  mentionsMe: boolean;
  serializedId: string;
}

export interface WhatsappGroupInterface {
  whatsappGroupId: string;
  title: string;
  lastMessage?: string;
  lastMessageAt?: number;
  unreadCount?: number;
  participantsCount?: number;
}

export interface WhatsappContact {
  chatId: string; // identificador único, ej: 5215512345678@c.us
  name: string;
  phoneNumber: string;
}

export interface SendResultInterface {
  ok: boolean;
  error?: string;
}

export interface WhatsappChatSummary {
  chatId: string;
  name: string;
  isGroup: boolean;
  lastMessage?: string;
  lastMessageAt?: number;
  unreadCount: number;
  participantsCount?: number;
  isSavedContact?: boolean;
}

/**
 * Claves de Redis para el estado "actual" de cada sesión (status y último QR).
 * El runtime ESCRIBE estas claves; la API solo LEE.
 * Centralizarlas aquí evita que un typo en un lado rompa la lectura del otro.
 */
export function whatsappStatusKey(connectionId: string): string {
  return `wa:status:${connectionId}`;
}

export function whatsappQrKey(connectionId: string): string {
  return `wa:qr:${connectionId}`;
}

/** Valor que el runtime guarda en `whatsappQrKey`: el QR ya como PNG en base64. */
export interface WhatsappQrCacheValue {
  qrPngBase64: string;
  generatedAt: number;
}
