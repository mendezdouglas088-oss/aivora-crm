import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Pool } from 'pg';
import * as fs from 'fs/promises';
import * as path from 'path';

/**
 * Implementa el contrato `Store` que pide `RemoteAuth` de whatsapp-web.js:
 *   sessionExists({ session })
 *   save({ session })
 *   extract({ session, path })
 *   delete({ session })
 *
 * Verificado contra el código fuente de RemoteAuth y contra implementaciones
 * de referencia (wwebjs-mongo, stores custom de S3): `save` espera encontrar
 * un zip YA CREADO por RemoteAuth en `process.cwd()/{session}.zip` — nuestro
 * trabajo es solo leerlo y guardarlo en Postgres. `extract` es lo inverso:
 * nos piden escribir los bytes guardados en el `path` local que nos dan.
 *
 * Usa `pg` directo (sin TypeORM) a propósito: este proceso no necesita
 * levantar todo el ORM solo para leer/escribir un blob.
 *
 * Reutiliza las mismas variables de entorno que ya usa
 * `src/config/database.config.ts` en la API, para no duplicar configuración.
 */
@Injectable()
export class WhatsappRemoteAuthStore implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WhatsappRemoteAuthStore.name);
  private readonly pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.DATABASE_HOST || 'localhost',
      port: parseInt(process.env.DATABASE_PORT || '5433', 10),
      user: process.env.DATABASE_USER || 'postgres',
      password: process.env.DATABASE_PASSWORD || 'postgres',
      database: process.env.DATABASE_NAME || 'crm_whatsapp',
    });
  }

  async onModuleInit(): Promise<void> {
    // CREATE TABLE IF NOT EXISTS por simplicidad, siguiendo el mismo espíritu
    // pragmático que `synchronize: true` en el TypeORM de la API (marcado
    // ahí mismo como "SOLO DEV"). Cuando formalicen migraciones, esta tabla
    // debería pasar a vivir en una migración de TypeORM como las demás.
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS whatsapp_session_blobs (
        session_id TEXT PRIMARY KEY,
        data BYTEA NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
    this.logger.log('Tabla whatsapp_session_blobs verificada');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }

  async sessionExists({ session }: { session: string }): Promise<boolean> {
    const result = await this.pool.query(
      'SELECT 1 FROM whatsapp_session_blobs WHERE session_id = $1',
      [session],
    );
    return (result.rowCount ?? 0) > 0;
  }

  async save({ session }: { session: string }): Promise<void> {
    const zipPath = this.localZipPath(session);
    const data = await fs.readFile(zipPath);
    await this.pool.query(
      `INSERT INTO whatsapp_session_blobs (session_id, data, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (session_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
      [session, data],
    );
    this.logger.debug(
      `[${session}] sesión respaldada en Postgres (${data.length} bytes)`,
    );
  }

  async extract({
    session,
    path: destPath,
  }: {
    session: string;
    path: string;
  }): Promise<void> {
    const result = await this.pool.query(
      'SELECT data FROM whatsapp_session_blobs WHERE session_id = $1',
      [session],
    );
    if (result.rowCount === 0) {
      throw new Error(`No hay sesión remota guardada para ${session}`);
    }
    await fs.writeFile(destPath, result.rows[0].data);
  }

  async delete({ session }: { session: string }): Promise<void> {
    await this.pool.query(
      'DELETE FROM whatsapp_session_blobs WHERE session_id = $1',
      [session],
    );
  }

  private localZipPath(session: string): string {
    return path.join(process.cwd(), `${session}.zip`);
  }
}
