import { Clock, Crypto, DateTime, Effect, Schema } from "effect";

import { EventSink } from "./event-sink.js";
import { RawEventSchema } from "./schemas.js";
import type { BodySchema, EventSource, IdentityMode } from "./schemas.js";
import { webCryptoLayer } from "./web-crypto.js";

export interface CaptureEventOptions<Body extends BodySchema> {
  readonly anonymousId: string;
  readonly body: Body;
  readonly host: string;
  readonly identityMode?: IdentityMode;
  readonly source?: EventSource;
}

const decodeRawEvent = Schema.decodeUnknownEffect(RawEventSchema);

export const captureEvent = <Body extends BodySchema>(options: CaptureEventOptions<Body>) => {
  const encodeBody = Schema.encodeEffect(options.body);

  return (body: Body["Type"]): Effect.Effect<void, never, EventSink> =>
    Effect.gen(function* recordEvent() {
      const sink = yield* EventSink;
      const crypto = yield* Crypto.Crypto;
      const now = yield* Clock.currentTimeMillis;

      const event = yield* decodeRawEvent({
        anonymousId: options.anonymousId,
        body: yield* encodeBody(body),
        identityMode: options.identityMode ?? "persistent",
        messageId: yield* crypto.randomUUIDv7,
        server: {
          host: options.host,
          receivedAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
        },
        source: options.source ?? "node",
        v: 1,
      });

      yield* sink.send([event]);
    }).pipe(
      Effect.provide(webCryptoLayer),
      Effect.catchCause((cause) => Effect.logWarning("event capture failed", cause)),
    );
};
