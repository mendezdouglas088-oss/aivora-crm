/**
 * Elige el nombre a guardar para un chat/grupo sin degradar uno bueno.
 *
 * Cuando el runtime todavía no conoce el nombre de un chat, lo llama por su
 * número (`5215512345678`) o por su id de grupo. Si en la BD ya hay un nombre
 * real (guardado por un sync o un evento anterior), ese placeholder no debe
 * pisarlo.
 */
export function pickChatName(
  existing: string | null | undefined,
  incoming: string | null | undefined,
  chatId: string,
): string {
  const bareId = chatId.split('@')[0];
  const inc = incoming?.trim();
  const incomingIsPlaceholder = !inc || inc === bareId;
  const old = existing?.trim();
  if (incomingIsPlaceholder && old) return old;
  return inc || old || bareId;
}
