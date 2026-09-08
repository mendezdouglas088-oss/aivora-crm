import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { bullConfig } from 'src/config/bullmq.config';
import {
  SendResultInterface,
  WhatsappChatSummary,
  WhatsappCommandResult,
  WhatsappConnectionStatus,
  WhatsappContact,
  WhatsappMediaPayload,
  WhatsappQrCacheValue,
  whatsappQrKey,
  whatsappStatusKey,
} from './../../shared/whatsapp-contracts';
import { WhatsappCommandsQueue } from '../infrastructure/jobs/whatsapp-commands.queue';

/**
 * Único punto de contacto que `WhatsappController` necesita conocer para
 * todo lo relacionado con WhatsApp. Reemplaza la inyección directa de
 * `WHATSAPP_PROVIDER` (Fase 0). Agrupa dos cosas de naturaleza distinta:
 *
 *  - Comandos (connect, send-text, logout, ...) → van por la cola y
 *    esperan respuesta del runtime.
 *  - Lecturas de estado (status, QR) → se leen directo de Redis, porque
 *    cambian constantemente y no tiene sentido pagar el costo de una cola
 *    para un simple GET. Las escribe el runtime (lo veremos al construir
 *    whatsapp-runtime.service.ts).
 *
 * Antes esto vivía repartido entre el `Map` en memoria de
 * `whatsapp-web.provider.ts` y las llamadas directas del controller.
 */
@Injectable()
export class WhatsappCommandsService implements OnModuleDestroy {
  private readonly logger = new Logger(WhatsappCommandsService.name);
  private readonly redis: Redis;

  constructor(private readonly commandsQueue: WhatsappCommandsQueue) {
    // Mismas credenciales que ya usa BullMQ (src/config/bullmq.config.ts),
    // pero como cliente de lectura simple, sin nada de colas.
    this.redis = new Redis(bullConfig.connection);
  }

  async onModuleDestroy() {
    this.redis.disconnect();
  }

  // ── estado (lectura directa de Redis, no son comandos) ─────────────

  async getStatus(connectionId: string): Promise<WhatsappConnectionStatus> {
    const status = await this.redis.get(whatsappStatusKey(connectionId));
    return (status as WhatsappConnectionStatus) ?? 'disconnected';
  }

  async getQr(connectionId: string): Promise<Buffer | null> {
    // Mismo criterio que el provider original: solo hay QR válido mientras
    // el estado sea 'waiting_qr'. Evita devolver un QR viejo tras reconectar.
    const status = await this.getStatus(connectionId);
    if (status !== 'waiting_qr') return null;

    const raw = await this.redis.get(whatsappQrKey(connectionId));
    if (!raw) return null;

    const cached: WhatsappQrCacheValue = JSON.parse(raw);
    return Buffer.from(cached.qrPngBase64, 'base64');
  }

  // ── comandos (van al runtime por la cola) ──────────────────────────

  async connect(
    connectionId: string,
  ): Promise<WhatsappCommandResult<'connect'>> {
    // Seguro de reintentar: el runtime ya ignora un 'connect' si la sesión
    // está conectando/conectada (misma guarda que tiene hoy `connect()`).
    return this.commandsQueue.send(
      { type: 'connect', connectionId },
      { attempts: 3 },
    );
  }

  async logout(connectionId: string): Promise<void> {
    await this.commandsQueue.send(
      { type: 'logout', connectionId },
      { attempts: 3 },
    );
  }

  async sendText(
    connectionId: string,
    chatId: string,
    text: string,
  ): Promise<SendResultInterface> {
    // attempts: 1 (por defecto) — nunca reintentamos un envío automáticamente.
    return this.commandsQueue.send({
      type: 'send-text',
      connectionId,
      chatId,
      text,
    });
  }

  async sendImages(
    connectionId: string,
    groupId: string,
    imageUrls: string[],
    caption?: string,
  ): Promise<SendResultInterface> {
    return this.commandsQueue.send({
      type: 'send-images',
      connectionId,
      groupId,
      imageUrls,
      caption,
    });
  }

  async sendMedia(
    connectionId: string,
    chatId: string,
    media: WhatsappMediaPayload,
    options?: { caption?: string; sendAudioAsVoice?: boolean },
  ): Promise<SendResultInterface> {
    return this.commandsQueue.send({
      type: 'send-media',
      connectionId,
      chatId,
      media,
      options,
    });
  }

  async getMedia(
    connectionId: string,
    messageId: string,
  ): Promise<WhatsappMediaPayload | null> {
    // Descargar multimedia puede tardar más que un comando normal.
    return this.commandsQueue.send(
      { type: 'get-media', connectionId, messageId },
      { timeoutMs: 60_000, attempts: 3 },
    );
  }

  async getContact(
    connectionId: string,
    chatId: string,
  ): Promise<WhatsappContact> {
    return this.commandsQueue.send(
      { type: 'get-contact', connectionId, chatId },
      { attempts: 3 },
    );
  }

  async syncAll(connectionId: string): Promise<WhatsappChatSummary[]> {
    // Recorre todos los chats/contactos de la cuenta — en cuentas grandes
    // puede tardar bastante más que el timeout por defecto.
    return this.commandsQueue.send(
      { type: 'sync-all', connectionId },
      { timeoutMs: 60_000, attempts: 3 },
    );
  }
}
