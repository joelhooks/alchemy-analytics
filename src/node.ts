import { Array as Arr, Effect, Layer, Queue, Redacted, Schema } from "effect";
import type { Duration } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";

import { EventSink } from "./event-sink.js";
import { IngestBatchSchema } from "./ingest.js";
import type { RawEvent } from "./schemas.js";

export const DEFAULT_MAX_BUFFER = 1000;

export const DEFAULT_MAX_BATCH = 100;

export const DEFAULT_FLUSH_INTERVAL: Duration.Input = "5 seconds";

export const DEFAULT_REQUEST_TIMEOUT: Duration.Input = "10 seconds";

export interface NodeEventSinkOptions {
  readonly flushInterval?: Duration.Input;
  readonly maxBatch?: number;
  readonly maxBuffer?: number;
  readonly requestTimeout?: Duration.Input;
  readonly secret: Redacted.Redacted;
  readonly url: string;
}

const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(IngestBatchSchema));

export const nodeEventSink = (options: NodeEventSinkOptions) =>
  Layer.effect(
    EventSink,
    Effect.gen(function* buildNodeEventSink() {
      const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
      const maxBatch = Math.max(1, options.maxBatch ?? DEFAULT_MAX_BATCH);

      const queue = yield* Queue.dropping<RawEvent>(
        Math.max(1, options.maxBuffer ?? DEFAULT_MAX_BUFFER),
      );

      const post = (events: readonly RawEvent[]) =>
        encodeBatch({ events }).pipe(
          Effect.flatMap((body) =>
            client.execute(
              HttpClientRequest.post(options.url).pipe(
                HttpClientRequest.bearerToken(Redacted.value(options.secret)),
                HttpClientRequest.bodyText(body, "application/json"),
              ),
            ),
          ),
          Effect.timeout(options.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT),
          Effect.catchCause((cause) =>
            Effect.logWarning(`event ingest dropped a batch of ${events.length}`, cause),
          ),
          Effect.asVoid,
        );

      const drain = Queue.clear(queue).pipe(
        Effect.flatMap((events) =>
          Effect.forEach(Arr.chunksOf(events, maxBatch), post, {
            discard: true,
          }),
        ),
      );

      yield* Effect.addFinalizer(() => drain);

      yield* Effect.forkScoped(
        Effect.forever(
          Effect.sleep(options.flushInterval ?? DEFAULT_FLUSH_INTERVAL).pipe(
            Effect.andThen(Effect.uninterruptible(drain)),
          ),
        ),
      );

      return {
        send: (events) =>
          Queue.offerAll(queue, events).pipe(
            Effect.flatMap((rejected) =>
              rejected.length === 0
                ? Effect.void
                : Effect.logWarning(`event buffer full; dropped ${rejected.length} events`),
            ),
          ),
      };
    }),
  );

export const NodeEventSink = (options: NodeEventSinkOptions) =>
  nodeEventSink(options).pipe(Layer.provide(FetchHttpClient.layer));
