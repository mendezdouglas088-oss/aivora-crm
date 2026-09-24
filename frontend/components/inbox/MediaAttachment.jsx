"use client";

import { useEffect, useRef, useState } from "react";
import { FileText, Mic, Video, Loader2, AlertCircle } from "lucide-react";
import { apiFetch } from "@/lib/api";

function base64ToBlob(base64, mimetype) {
  const byteChars = atob(base64);
  const byteNumbers = new Array(byteChars.length);
  for (let i = 0; i < byteChars.length; i++) {
    byteNumbers[i] = byteChars.charCodeAt(i);
  }
  return new Blob([new Uint8Array(byteNumbers)], { type: mimetype });
}

export function MediaAttachment({
  type,
  hasMedia,
  serializedId,
  connectionId,
  caption,
}) {
  const [state, setState] = useState("idle"); // idle | loading | ready | error | unavailable
  const [mediaUrl, setMediaUrl] = useState(null);
  const [filename, setFilename] = useState(null);
  const requestedRef = useRef(false); // evita doble fetch (StrictMode / re-renders)

  const autoLoad = type === "image" || type === "sticker";

  async function loadMedia() {
    if (requestedRef.current) return;
    requestedRef.current = true;
    setState("loading");
    try {
      const res = await apiFetch(
        `/whatsapp/media/${encodeURIComponent(serializedId)}?connectionId=${connectionId}`,
      );
      const text = await res.text();
      const data = text ? JSON.parse(text) : null;
      if (!res.ok || !data?.data) {
        setState("unavailable");
        return;
      }
      const blob = base64ToBlob(data.data, data.mimetype);
      setMediaUrl(URL.createObjectURL(blob));
      setFilename(data.filename || null);
      setState("ready");
    } catch (err) {
      console.error("Error cargando media", err);
      requestedRef.current = false;
      setState("error");
    }
  }

  useEffect(
    () =>
      async function () {
        if (autoLoad && serializedId) await loadMedia();
        // eslint-disable-next-line react-hooks/exhaustive-deps
      },
    [serializedId],
  );

  useEffect(() => {
    return () => {
      if (mediaUrl) URL.revokeObjectURL(mediaUrl);
    };
  }, [mediaUrl]);

  if (!hasMedia) return null;

  if (!serializedId) {
    return (
      <div className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-2.5 py-1.5 text-xs text-neutral-400 dark:text-neutral-500">
        <AlertCircle className="h-3.5 w-3.5" />
        Media no disponible (mensaje anterior a la actualización)
      </div>
    );
  }

  if (state === "unavailable") {
    return (
      <div className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-2.5 py-1.5 text-xs text-neutral-400 dark:text-neutral-500">
        <AlertCircle className="h-3.5 w-3.5" />
        El archivo ya no está disponible
      </div>
    );
  }

  if (state === "error") {
    return (
      <button
        type="button"
        onClick={loadMedia}
        className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-red-50 dark:bg-red-500/10 px-2.5 py-1.5 text-xs text-red-500 hover:bg-red-100 dark:hover:bg-red-500/15"
      >
        <AlertCircle className="h-3.5 w-3.5" />
        Error al cargar, reintentar
      </button>
    );
  }

  // imágenes y stickers: preview automático
  if (type === "image" || type === "sticker") {
    if (state !== "ready") {
      return (
        <div className="mt-1.5 flex h-32 w-48 items-center justify-center rounded-lg bg-neutral-100 dark:bg-neutral-800">
          <Loader2 className="h-4 w-4 animate-spin text-neutral-400 dark:text-neutral-500" />
        </div>
      );
    }
    return (
      <a href={mediaUrl} target="_blank" rel="noopener noreferrer">
        <img
          src={mediaUrl}
          alt={caption || "imagen"}
          className="mt-1.5 max-h-64 max-w-xs rounded-lg object-cover"
        />
      </a>
    );
  }

  // video: bajo demanda
  if (type === "video") {
    return state === "ready" ? (
      <video
        src={mediaUrl}
        controls
        className="mt-1.5 max-h-64 max-w-xs rounded-lg"
      />
    ) : (
      <button
        type="button"
        onClick={loadMedia}
        disabled={state === "loading"}
        className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-3 py-2 text-xs text-neutral-600 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700"
      >
        {state === "loading" ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <Video className="h-3.5 w-3.5" />
        )}
        Ver video
      </button>
    );
  }

  // audio / nota de voz: bajo demanda
  if (type === "audio" || type === "ptt") {
    return state === "ready" ? (
      <audio src={mediaUrl} controls className="mt-1.5 h-10 max-w-xs" />
    ) : (
      <button
        type="button"
        onClick={loadMedia}
        disabled={state === "loading"}
        className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-3 py-2 text-xs text-neutral-600 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700"
      >
        {state === "loading" ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <Mic className="h-3.5 w-3.5" />
        )}
        Reproducir audio
      </button>
    );
  }

  // documento y cualquier otro tipo con media: descarga
  return state === "ready" ? (
    <a
      href={mediaUrl}
      download={filename || "archivo"}
      className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-3 py-2 text-xs text-neutral-600 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700"
    >
      <FileText className="h-3.5 w-3.5" />
      {filename || "Descargar archivo"}
    </a>
  ) : (
    <button
      type="button"
      onClick={loadMedia}
      disabled={state === "loading"}
      className="mt-1.5 flex items-center gap-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 px-3 py-2 text-xs text-neutral-600 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700"
    >
      {state === "loading" ? (
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
      ) : (
        <FileText className="h-3.5 w-3.5" />
      )}
      Descargar {filename || "archivo"}
    </button>
  );
}
