import {
  isLidUser,
  isPnUser,
  jidNormalizedUser,
} from '@whiskeysockets/baileys';

/**
 * Identidad de chats 1:1 en Baileys v7.
 *
 * En v7 una misma persona puede aparecer con DOS identificadores:
 *   - `<numero>@s.whatsapp.net`  (PN, su número de teléfono)
 *   - `<numero>@lid`             (LID, id opaco que WhatsApp usa cada vez más)
 *
 * Los eventos de Baileys no son consistentes sobre cuál usan: los mensajes
 * pueden venir por LID mientras que `chats.upsert` / `messaging-history.set`
 * (chats y contactos) vienen por PN, o al revés, y el mapeo LID↔PN a veces
 * se conoce DESPUÉS de haber creado el chat. Si cada camino arma su propio
 * chatId, la misma persona termina como dos chats: uno con el historial y
 * otro vacío (justo el síntoma reportado).
 *
 * Esta clase es el único lugar que decide cuál es el chatId "canónico":
 * el PN en formato legacy (`@c.us`) cuando se conoce, y si no, el LID.
 * No depende del socket, así que se puede probar aislada.
 */

const LEGACY_INDIVIDUAL_SUFFIX = '@c.us';
const BAILEYS_INDIVIDUAL_SUFFIX = '@s.whatsapp.net';

/** Cuánto se recuerda que Baileys NO conocía el PN de un LID (evita un lookup por mensaje). */
const MISS_TTL_MS = 30_000;

export function toLegacyId(jid?: string | null): string {
  if (!jid) return '';
  if (jid.endsWith(BAILEYS_INDIVIDUAL_SUFFIX)) {
    return (
      jid.slice(0, -BAILEYS_INDIVIDUAL_SUFFIX.length) + LEGACY_INDIVIDUAL_SUFFIX
    );
  }
  return jid;
}

export function toBaileysJid(legacyId: string): string {
  if (legacyId.endsWith(LEGACY_INDIVIDUAL_SUFFIX)) {
    return (
      legacyId.slice(0, -LEGACY_INDIVIDUAL_SUFFIX.length) +
      BAILEYS_INDIVIDUAL_SUFFIX
    );
  }
  return legacyId;
}

export function jidUser(jid: string): string {
  return jid.split('@')[0]?.split(':')[0] ?? jid;
}

/** true si el "nombre" es solo el número/LID del propio chat (o está vacío). */
export function isPlaceholderName(
  name: string | undefined | null,
  ...ids: string[]
): boolean {
  if (!name) return true;
  return ids.some((id) => !!id && name === jidUser(id));
}

export class ChatIdentityMap {
  /** `123@lid` → `5215512345678@c.us` */
  private readonly lidToPn = new Map<string, string>();
  /** `5215512345678@c.us` → { `123@lid`, ... } */
  private readonly aliases = new Map<string, Set<string>>();
  private readonly misses = new Map<string, number>();

  /**
   * Registra que `lid` y `pn` son la misma persona. Devuelve el par
   * normalizado SOLO si el mapeo era nuevo (para que quien llama fusione lo
   * que ya haya guardado bajo el LID); `null` si era inválido o ya conocido.
   */
  learn(
    lid: string | null | undefined,
    pn: string | null | undefined,
  ): { lid: string; canonical: string } | null {
    if (!lid || !pn) return null;
    const lidJid = jidNormalizedUser(lid);
    const pnJid = jidNormalizedUser(pn);
    if (!isLidUser(lidJid) || !isPnUser(pnJid)) return null;

    const canonical = toLegacyId(pnJid);
    if (this.lidToPn.get(lidJid) === canonical) return null;

    this.lidToPn.set(lidJid, canonical);
    this.misses.delete(lidJid);
    let set = this.aliases.get(canonical);
    if (!set) this.aliases.set(canonical, (set = new Set()));
    set.add(lidJid);
    return { lid: lidJid, canonical };
  }

  /** chatId canónico (formato legacy) de cualquier JID. */
  canonical(jid?: string | null): string {
    if (!jid) return '';
    const normalized = jidNormalizedUser(jid);
    if (isLidUser(normalized)) {
      return this.lidToPn.get(normalized) ?? normalized;
    }
    return toLegacyId(normalized);
  }

  /** Otros ids (LID) que se sabe que son la misma persona que `canonical`. */
  aliasesOf(canonical: string): string[] {
    return [...(this.aliases.get(canonical) ?? [])];
  }

  recentlyMissed(lid: string, now = Date.now()): boolean {
    const until = this.misses.get(lid);
    if (until === undefined) return false;
    if (now >= until) {
      this.misses.delete(lid);
      return false;
    }
    return true;
  }

  markMiss(lid: string, now = Date.now()): void {
    this.misses.set(lid, now + MISS_TTL_MS);
  }
}
