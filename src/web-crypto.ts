import { Clock, Effect } from "effect";
import * as Digest from "effect/crypto/Digest";

export const webCryptoLayer = Digest.layerWebCrypto;

const UUID_BYTES = 16;
const TIMESTAMP_BYTES = 6;
const MAX_TIMESTAMP = 2 ** (TIMESTAMP_BYTES * 8) - 1;

const toHex = (bytes: Uint8Array) =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

/**
 * A UUIDv7 per RFC 9562: 48 bits of Unix milliseconds, then version 7, then variant 10,
 * and random bits elsewhere. Built here so capture needs no `SecureRandom` service from the
 * caller: Effect 4.1 moved UUIDv7 from `Crypto.randomUUIDv7` into the `SecureRandom` service.
 */
export const uuidV7Of = (millis: number, random: Uint8Array) => {
  const bytes = new Uint8Array(UUID_BYTES);
  // Float division, not a bitwise shift: the timestamp exceeds 32 bits.
  let remaining = Math.min(Math.max(Math.trunc(millis), 0), MAX_TIMESTAMP);

  for (let index = TIMESTAMP_BYTES - 1; index >= 0; index -= 1) {
    bytes[index] = remaining % 256;
    remaining = Math.floor(remaining / 256);
  }

  bytes.set(random.subarray(0, UUID_BYTES - TIMESTAMP_BYTES), TIMESTAMP_BYTES);
  bytes[6] = 0x70 + ((bytes[6] ?? 0) % 16);
  bytes[8] = 0x80 + ((bytes[8] ?? 0) % 64);

  const hex = toHex(bytes);

  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};

export const randomUuidV7: Effect.Effect<string> = Clock.currentTimeMillis.pipe(
  Effect.map((now) => uuidV7Of(now, globalThis.crypto.getRandomValues(new Uint8Array(UUID_BYTES)))),
);
