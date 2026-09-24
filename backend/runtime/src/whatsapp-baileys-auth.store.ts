import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Pool } from 'pg';
import {
  AuthenticationCreds,
  AuthenticationState,
  BufferJSON,
  SignalDataTypeMap,
  initAuthCreds,
  proto,
} from '@whiskeysockets/baileys';

export interface WhatsappBaileysAuthState {
  state: AuthenticationState;
  saveCreds: () => Promise<void>;
}

/**
 * Equivalente a `WhatsappRemoteAuthStore` (el `Store` que pedía `RemoteAuth`
 * de whatsapp-web.js) pero para Baileys. La API de auth es distinta:
 * Baileys no tiene el concepto de "Store" con save/extract de un zip —
 * su primitiva nativa es `useMultiFileAuthState(folder)`, que lee/escribe
 * un archivo `creds.json` + un archivo por cada clave de Signal (pre-keys,
 * sender-keys, sesiones, etc.) en una carpeta local.
 *
 * `getAuthState()` de abajo es ese mismo contrato (`{ state, saveCreds }`
 * que `makeWASocket({ auth })` espera recibir) pero contra Postgres en vez
 * de disco — mismo motivo que tenía `RemoteAuth` antes: este runtime no
 * puede depender del disco de un container puntual si va a reiniciarse o
 * correr en más de una instancia.
 *
 * Una fila por cada `(connectionId, keyId)`: `keyId = 'creds'` para las
 * credenciales, `keyId = '<tipo>-<id>'` (ej. `session-573...@s.whatsapp.net`,
 * `app-state-sync-key-abc123`) para cada entrada del `SignalKeyStore`.
 * `BufferJSON.replacer`/`.reviver` (que exporta la propia librería) hacen
 * el mismo trabajo que hace `useMultiFileAuthState` al serializar a disco:
 * sin esto, los `Buffer` de las claves de Signal se guardarían como objetos
 * vacíos y la sesión no podría descifrar nada.
 *
 * Reutiliza las mismas variables de entorno que ya usa
 * `src/config/database.config.ts` en la API — igual que hacía
 * `WhatsappRemoteAuthStore` — y usa `pg` directo por el mismo motivo: no
 * hace falta levantar TypeORM para leer/escribir filas sueltas.
 */
@Injectable()
export class WhatsappBaileysAuthStore implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WhatsappBaileysAuthStore.name);
  private readonly pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.DATABASE_HOST || 'localhost',
      port: parseInt(process.env.DATABASE_PORT || '5433', 10),
      user: process.env.DATABASE_USER || 'postgres',
      password: process.env.DATABASE_PASSWORD || 'postgres',
      database: process.env.DATABASE_NAME || 'crm_whatsapp',
      connectionTimeoutMillis: 10_000,
    });
    this.pool.on('error', (err) => {
      this.logger.error(
        `Error inesperado en el pool de Postgres: ${err.message}`,
      );
    });
  }

  async onModuleInit(): Promise<void> {
    await this.runWithRetry(() =>
      this.pool.query(`
      CREATE TABLE IF NOT EXISTS whatsapp_baileys_auth (
        connection_id TEXT NOT NULL,
        key_id TEXT NOT NULL,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (connection_id, key_id)
      );
    `),
    );
    this.logger.log('Tabla whatsapp_baileys_auth verificada');
  }

  private async runWithRetry<T>(
    fn: () => Promise<T>,
    attempts = 5,
    delayMs = 2000,
  ): Promise<T> {
    for (let i = 1; i <= attempts; i++) {
      try {
        return await fn();
      } catch (e) {
        if (i === attempts) throw e;
        this.logger.warn(
          `Postgres no respondió (intento ${i}/${attempts}): ${e.message}. Reintentando en ${delayMs}ms...`,
        );
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }
    throw new Error('unreachable');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }

  /**
   * Carga (o crea, si es la primera vez) el estado de auth de una conexión.
   * `makeWASocket({ auth: state })` usa `state` directo; hay que suscribir
   * `saveCreds` al evento `creds.update` del socket para que las
   * credenciales se persistan cada vez que Baileys las actualiza.
   */
  async getAuthState(connectionId: string): Promise<WhatsappBaileysAuthState> {
    const storedCreds = await this.readKey(connectionId, 'creds');
    const creds: AuthenticationCreds = storedCreds ?? initAuthCreds();

    const state: AuthenticationState = {
      creds,
      keys: {
        get: async (type, ids) => {
          const result: { [id: string]: any } = {};
          await Promise.all(
            ids.map(async (id) => {
              const value = await this.readKey(connectionId, `${type}-${id}`);
              if (value !== null) {
                // Igual que useMultiFileAuthState: las app-state-sync-key
                // hay que rehidratarlas a su clase proto, si no el sync de
                // app state (nombres de contactos, chats archivados, etc.) falla.
                result[id] =
                  type === 'app-state-sync-key'
                    ? proto.Message.AppStateSyncKeyData.fromObject(value)
                    : value;
              }
            }),
          );
          return result;
        },
        set: async (data) => {
          const tasks: Promise<void>[] = [];
          for (const type of Object.keys(data) as (keyof SignalDataTypeMap)[]) {
            const entries = data[type] as { [id: string]: unknown };
            for (const id of Object.keys(entries)) {
              const value = entries[id];
              const keyId = `${type}-${id}`;
              tasks.push(
                value
                  ? this.writeKey(connectionId, keyId, value)
                  : this.deleteKey(connectionId, keyId),
              );
            }
          }
          await Promise.all(tasks);
        },
        // Parte del contrato `SignalKeyStore` (opcional, pero la
        // implementación de referencia de Baileys la soporta) — borra solo
        // las claves de Signal de esta conexión, sin tocar la fila `creds`
        // (esa vive aparte, en `AuthenticationState.creds`, no en `.keys`).
        clear: async () => {
          await this.pool.query(
            "DELETE FROM whatsapp_baileys_auth WHERE connection_id = $1 AND key_id != 'creds'",
            [connectionId],
          );
        },
      },
    };

    const saveCreds = () => this.writeKey(connectionId, 'creds', state.creds);

    return { state, saveCreds };
  }

  async sessionExists(connectionId: string): Promise<boolean> {
    const creds = await this.readKey(connectionId, 'creds');
    return creds !== null;
  }

  /** Borra TODAS las filas (creds + claves de Signal) de una conexión — usar en logout(). */
  async delete(connectionId: string): Promise<void> {
    await this.pool.query(
      'DELETE FROM whatsapp_baileys_auth WHERE connection_id = $1',
      [connectionId],
    );
  }

  private async readKey(
    connectionId: string,
    keyId: string,
  ): Promise<any | null> {
    const result = await this.pool.query(
      'SELECT data FROM whatsapp_baileys_auth WHERE connection_id = $1 AND key_id = $2',
      [connectionId, keyId],
    );
    if (result.rowCount === 0) return null;
    // El JSONB vuelve de pg ya parseado a objeto plano — hay que pasarlo de
    // nuevo por BufferJSON.reviver para reconstruir los Buffer que
    // BufferJSON.replacer aplanó a `{ type: 'Buffer', data: [...] }` al guardar.
    return JSON.parse(JSON.stringify(result.rows[0].data), BufferJSON.reviver);
  }

  private async writeKey(
    connectionId: string,
    keyId: string,
    value: unknown,
  ): Promise<void> {
    const serializable = JSON.parse(JSON.stringify(value, BufferJSON.replacer));
    await this.pool.query(
      `INSERT INTO whatsapp_baileys_auth (connection_id, key_id, data, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (connection_id, key_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
      [connectionId, keyId, serializable],
    );
  }

  private async deleteKey(connectionId: string, keyId: string): Promise<void> {
    await this.pool.query(
      'DELETE FROM whatsapp_baileys_auth WHERE connection_id = $1 AND key_id = $2',
      [connectionId, keyId],
    );
  }
}
