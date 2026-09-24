import {
  WhatsappConnectionStatus,
  WhatsappHistorySyncDonePayload,
  WhatsappMessagePersistPayload,
} from './types';

/**
 * Cola BullMQ para eventos que necesitan reintentos si falla su procesamiento
 * (hoy: solo guardar el mensaje en Postgres). Antes esto era un `emit()` local
 * escuchado por `whatsapp-events.listener.ts`; ahora es una cola real porque
 * cruza de proceso y un fallo de guardado sí debe reintentarse.
 */
export const WHATSAPP_PERSIST_EVENTS_QUEUE = 'whatsapp-persist-events';
export const WHATSAPP_PERSIST_EVENT_JOB_NAME = 'message-persist';

/**
 * Canal de Redis Pub/Sub para eventos "en caliente": si nadie los escucha en
 * el momento exacto en que ocurren, no pasa nada (a diferencia de la cola de
 * persistencia). Alimentan directamente al WebSocket vía
 * `whatsapp-live-events.bridge.ts`, que los re-emite como los `@OnEvent(...)`
 * que `realtime.gateway.ts` ya escucha hoy — por eso el gateway no cambia.
 */
export const WHATSAPP_LIVE_EVENTS_CHANNEL = 'whatsapp-live-events';

export const WHATSAPP_HISTORY_BATCH_JOB_NAME = 'history-batch-persist';
export const WHATSAPP_HISTORY_DONE_JOB_NAME = 'history-sync-done';

export interface WhatsappHistoryBatchJob {
  messages: WhatsappMessagePersistPayload[];
}

export interface WhatsappHistoryDoneJob {
  payload: WhatsappHistorySyncDonePayload;
}

export interface WhatsappQrEvent {
  kind: 'qr';
  connectionId: string;
  /** Ya viene como data URL (`data:image/png;base64,...`), igual que hoy. */
  qr: string;
}

export interface WhatsappStatusEvent {
  kind: 'status';
  connectionId: string;
  status: WhatsappConnectionStatus;
}

export interface WhatsappCallEvent {
  kind: 'call';
  connectionId: string;
  from: string;
  isVideo: boolean;
  isGroup: boolean;
  timestamp: number;
}

export interface WhatsappMessageReceivedEvent {
  kind: 'message-received';
  connectionId: string;
  chatId: string;
  isGroup: boolean;
  contact: {
    chatId: string;
    name: string;
    phoneNumber: string;
  };
  text: string;
  messageId: string;
  serializedId: string;
  type: string;
  hasMedia: boolean;
}

/** Unión de todo lo que puede llegar por el canal de eventos en vivo. */
export type WhatsappLiveEvent =
  | WhatsappQrEvent
  | WhatsappStatusEvent
  | WhatsappCallEvent
  | WhatsappMessageReceivedEvent;

/** Job que viaja por la cola de persistencia — mismo payload que ya existía. */
export interface WhatsappPersistEventJob {
  payload: WhatsappMessagePersistPayload;
}
