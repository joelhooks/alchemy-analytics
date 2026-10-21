import { Crypto, Effect, Redacted, Schema, Stream } from "effect";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { EventSink } from "./event-sink.js";
import { RawEventSchema } from "./schemas.js";
import { webCryptoLayer } from "./web-crypto.js";

export const INGEST_PATH = "/_analytics/ingest";

export const DEFAULT_MAX_BODY_BYTES = 1_048_576;

export const DEFAULT_MAX_EVENTS = 500;

export interface IngestOptions {
  readonly maxBodyBytes?: number;
  readonly maxEvents?: number;
  readonly path?: string;
  readonly secret: Redacted.Redacted;
}

export const IngestBatchSchema = Schema.Struct({
  events: Schema.Array(RawEventSchema),
});

export type IngestBatch = typeof IngestBatchSchema.Type;

class IngestRejection extends Schema.TaggedError<IngestRejection>()("IngestRejection", {
  reason: Schema.String,
  status: Schema.Int,
}) {}

const reject = (status: number, reason: string) =>
  Effect.fail(new IngestRejection({ reason, status }));

const encoder = new TextEncoder();

const bearerOf = (header: string | undefined) =>
  header?.startsWith("Bearer ") === true ? header.slice(7) : "";

export const secretMatches = (presented: string, secret: Redacted.Redacted) =>
  Effect.gen(function* compareSecrets() {
    const crypto = yield* Crypto.Crypto;

    const [left, right] = yield* Effect.all([
      crypto.digest("SHA-256", encoder.encode(presented)),
      crypto.digest("SHA-256", encoder.encode(Redacted.value(secret))),
    ]);

    // oxlint-disable-next-line eslint/no-bitwise -- constant-time digest comparison needs XOR accumulation.
    let difference = left.length ^ right.length;

    for (const [index, byte] of left.entries()) {
      // oxlint-disable-next-line eslint/no-bitwise -- constant-time digest comparison needs XOR accumulation.
      difference |= byte ^ (right[index] ?? 0);
    }

    return difference === 0 && Redacted.value(secret).length > 0;
  }).pipe(
    Effect.provide(webCryptoLayer),
    Effect.orElseSucceed(() => false),
  );

const declaredLength = (header: string | undefined) => {
  const length = Number(header);

  return header !== undefined && Number.isSafeInteger(length) && length >= 0 ? length : undefined;
};

const readCapped = (request: HttpServerRequest.HttpServerRequest, maxBytes: number) =>
  Effect.suspend(() => {
    const chunks: Uint8Array[] = [];

    return request.stream.pipe(
      Stream.mapError(() => new IngestRejection({ reason: "unreadable body", status: 400 })),
      Stream.runFoldEffect(
        () => 0,
        (size, chunk) => {
          if (size + chunk.length > maxBytes) {
            return reject(413, "body too large");
          }

          chunks.push(chunk);

          return Effect.succeed(size + chunk.length);
        },
      ),
      Effect.map((size) => {
        const bytes = new Uint8Array(size);
        let offset = 0;

        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.length;
        }

        return new TextDecoder().decode(bytes);
      }),
    );
  });

const decodeEnvelope = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ events: Schema.Array(Schema.Unknown) })),
);

const decodeEvents = Schema.decodeUnknownEffect(Schema.Array(RawEventSchema));

const respond = (status: number, body: Readonly<Record<string, number | string>>) =>
  HttpServerResponse.text(JSON.stringify(body), {
    contentType: "application/json",
    status,
  });

export const ingestEvents = (options: IngestOptions) =>
  Effect.gen(function* handleIngest() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const maxBodyBytes = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
    const maxEvents = options.maxEvents ?? DEFAULT_MAX_EVENTS;

    if (request.method !== "POST") {
      return yield* reject(405, "method not allowed");
    }

    const authorized = yield* secretMatches(
      bearerOf(request.headers.authorization),
      options.secret,
    );

    if (!authorized) {
      return yield* reject(401, "unauthorized");
    }

    const length = declaredLength(request.headers["content-length"]);

    if (length !== undefined && length > maxBodyBytes) {
      return yield* reject(413, "body too large");
    }

    const text = yield* readCapped(request, maxBodyBytes);

    const envelope = yield* decodeEnvelope(text).pipe(
      Effect.mapError(() => new IngestRejection({ reason: "invalid batch", status: 400 })),
    );

    if (envelope.events.length > maxEvents) {
      return yield* reject(413, "too many events");
    }

    const events = yield* decodeEvents(envelope.events).pipe(
      Effect.mapError(() => new IngestRejection({ reason: "invalid event", status: 400 })),
    );

    if (events.length > 0) {
      const sink = yield* EventSink;

      yield* sink.send(events).pipe(
        Effect.tapError((error) => Effect.logWarning("event sink rejected an ingest batch", error)),
        Effect.mapError(() => new IngestRejection({ reason: "sink unavailable", status: 502 })),
      );
    }

    return respond(202, { accepted: events.length });
  }).pipe(
    Effect.catchTag("IngestRejection", ({ reason, status }) =>
      Effect.succeed(
        status === 401
          ? HttpServerResponse.setHeader(
              respond(status, { error: reason }),
              "www-authenticate",
              "Bearer",
            )
          : respond(status, { error: reason }),
      ),
    ),
  );

export const withIngestRoute =
  (options: IngestOptions) =>
  <E, R>(app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>) =>
    Effect.gen(function* routeIngest() {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const pathname = URL.canParse(request.url, "https://localhost")
        ? new URL(request.url, "https://localhost").pathname
        : undefined;

      return pathname === (options.path ?? INGEST_PATH) ? yield* ingestEvents(options) : yield* app;
    });
