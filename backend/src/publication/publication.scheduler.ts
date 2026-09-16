import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { ConfigService } from 'src/config/config.service';
import { PublicationService } from './publication.service';
import { TelegramGroupsService } from 'src/telegram-group/telegram-group.service';
import { UserbotClientService } from 'src/userbot/userbot-client.service';
import axios from 'axios';
import { WhatsappCommandsService } from 'src/whatsapp/services/whatsapp-commands.service';

/**
 * Scheduler de publicaciones automáticas.
 *
 * Para Telegram usa el USERBOT (cuenta real) cuando está conectado,
 * lo que permite enviar imágenes como álbum igual que el Python original.
 * Si el userbot no está disponible, cae al Bot API (solo texto).
 *
 * Para WhatsApp sigue usando WhatsappConnectService (whatsapp-web.js).
 */
@Injectable()
export class PublicationScheduler implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PublicationScheduler.name);
  private readonly botToken = process.env.TELEGRAM_BOT_TOKEN;
  private lastPublishedAt: Date | null = null;
  private intervalHandle: NodeJS.Timeout | null = null;
  private readonly CHECK_INTERVAL_MS = 30_000;

  constructor(
    private readonly configService: ConfigService,
    private readonly publicationService: PublicationService,
    private readonly telegramGroupsService: TelegramGroupsService,
    private readonly whatsappCommands: WhatsappCommandsService,
    // @Optional() — el userbot puede no estar disponible aún en el startup
    @Optional() private readonly userbotClient: UserbotClientService,
  ) {}

  onModuleInit() {
    this.logger.log('📅 Scheduler de publicaciones iniciado (check cada 30 s)');
    this.intervalHandle = setInterval(
      () => this.tick(),
      this.CHECK_INTERVAL_MS,
    );
  }

  onModuleDestroy() {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.logger.log('⏹️ Scheduler de publicaciones detenido');
    }
  }

  // ─────────────────────────────────────────────
  // TICK PRINCIPAL
  // ─────────────────────────────────────────────

  private async tick(): Promise<void> {
    try {
      const config = await this.configService.getConfig();

      if (!config.publishEnabled) return;

      const intervalMs = (config.publishInterval || 600) * 1_000;
      const now = new Date();

      if (
        this.lastPublishedAt &&
        now.getTime() - this.lastPublishedAt.getTime() < intervalMs
      ) {
        return;
      }

      this.logger.log('🚀 Iniciando ciclo de publicación...');
      this.lastPublishedAt = now;

      const publications = await this.publicationService.findAllActive();
      const active = publications.filter((p) => p.active);

      if (active.length === 0) {
        this.logger.debug('ℹ️ No hay publicaciones activas, saltando ciclo.');
        return;
      }

      this.logger.log(`📢 Enviando ${active.length} publicación(es)...`);
      for (const publication of active) {
        await this.publishOne(publication);
      }
      this.logger.log('✅ Ciclo de publicación completado');
    } catch (error: any) {
      this.logger.error('❌ Error en ciclo de publicación:', error?.message);
    }
  }

  // ─────────────────────────────────────────────
  // PUBLICAR UNA PUBLICACIÓN
  // ─────────────────────────────────────────────

  private async publishOne(publication: any): Promise<void> {
    const message = publication.description || publication.name;
    const products = publication.products || [];
    const productImages = products
      .map((p: any) => p.imageUrl)
      .filter((url: any): url is string => !!url && url.trim().length > 0);

    // telegramId del dueño de la publicación (para sesión WhatsApp correcta)
    const ownerTelegramId: string =
      publication.user?.telegramId ?? publication.userId ?? '';

    for (const dbId of (publication.telegramGroupIds || []) as string[]) {
      await this.sendToTelegram(dbId, publication.name, message, productImages);
    }

    for (const groupId of (publication.whatsappGroupIds || []) as string[]) {
      await this.sendToWhatsapp(
        groupId,
        publication.name,
        message,
        productImages,
        ownerTelegramId,
      );
    }
  }

  // ─────────────────────────────────────────────
  // ENVÍO A TELEGRAM
  // ─────────────────────────────────────────────

  /**
   * Estrategia de envío a Telegram:
   *  1. Userbot conectado + hay imágenes → GramJS sendFiles (álbum)
   *  2. Userbot conectado + sin imágenes → GramJS sendMessage
   *  3. Sin userbot + hay imágenes       → Bot API sendPhoto
   *  4. Sin userbot + sin imágenes       → Bot API sendMessage
   */
  private async sendToTelegram(
    dbId: string,
    publicationName: string,
    message: string,
    productImages: string[],
  ): Promise<void> {
    try {
      const group = await this.telegramGroupsService.findOne(Number(dbId));

      if (!group) {
        this.logger.warn(`⚠️ [Telegram] Grupo id=${dbId} no encontrado`);
        return;
      }

      const chatId = group.telegramGroupId;

      // ── Opción 1 y 2: Userbot GramJS ──────────────────────────────────────
      if (this.userbotClient?.isConnected()) {
        if (productImages.length > 0) {
          await this.userbotClient.sendFiles(chatId, productImages, message);
          this.logger.log(
            `✅ [Telegram/Userbot] "${publicationName}" (${productImages.length} img) → ${group.title}`,
          );
        } else {
          await this.userbotClient.sendMessage(chatId, message);
          this.logger.log(
            `✅ [Telegram/Userbot] "${publicationName}" (texto) → ${group.title}`,
          );
        }
        return;
      }

      // ── Opciones 3 y 4: Bot API fallback ──────────────────────────────────
      if (productImages.length > 0) {
        // Enviar primera imagen con caption, el resto sin
        await axios.post(
          `https://api.telegram.org/bot${this.botToken}/sendPhoto`,
          {
            chat_id: chatId,
            photo: productImages[0],
            caption: message,
            parse_mode: 'Markdown',
          },
        );
        for (let i = 1; i < productImages.length; i++) {
          await axios.post(
            `https://api.telegram.org/bot${this.botToken}/sendPhoto`,
            { chat_id: chatId, photo: productImages[i] },
          );
        }
      } else {
        await axios.post(
          `https://api.telegram.org/bot${this.botToken}/sendMessage`,
          { chat_id: chatId, text: message, parse_mode: 'Markdown' },
        );
      }

      this.logger.log(
        `✅ [Telegram/BotAPI] "${publicationName}" → ${group.title} (${chatId})`,
      );
    } catch (error: any) {
      this.logger.error(
        `❌ [Telegram] Error en "${publicationName}" → grupo id=${dbId}: ${error?.message}`,
      );
    }
  }

  // ─────────────────────────────────────────────
  // ENVÍO A WHATSAPP
  // ─────────────────────────────────────────────

  private async sendToWhatsapp(
    groupId: string,
    publicationName: string,
    message: string,
    productImages: string[],
    ownerTelegramId: string,
  ): Promise<void> {
    try {
      if (!ownerTelegramId) {
        this.logger.warn(
          `⚠️ [WhatsApp] Sin telegramId del dueño, saltando "${publicationName}"`,
        );
        return;
      }

      const status = await this.whatsappCommands.getStatus(ownerTelegramId);

      if (status !== 'connected') {
        this.logger.warn(
          `⚠️ [WhatsApp] Usuario ${ownerTelegramId} no conectado, saltando "${publicationName}"`,
        );
        return;
      }

      if (!productImages || productImages.length === 0) {
        this.logger.warn(
          `⚠️ [WhatsApp] Sin imágenes para "${publicationName}"`,
        );
        return;
      }

      const result = await this.whatsappCommands.sendImages(
        ownerTelegramId,
        groupId,
        productImages,
        message,
      );

      if ('error' in result) {
        this.logger.error(
          `❌ [WhatsApp] Error en "${publicationName}" → ${groupId}: ${result.error}`,
        );
        return;
      }

      this.logger.log(
        `✅ [WhatsApp] "${publicationName}" (${productImages.length} img) → ${groupId}`,
      );
    } catch (error: any) {
      this.logger.error(
        `❌ [WhatsApp] Error en "${publicationName}" → ${groupId}: ${error?.message}`,
      );
    }
  }
}
