import { Array as Arr, Effect, Layer, Queue, Redacted, Ref, Schema } from "effect";
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

export const DEFAULT_CLOSE_TIMEOUT: Duration.Input = "10 seconds";

export interface NodeEventSinkOptions {
  /** Longest time scope close may spend stopping the loop and flushing. */
  readonly closeTimeout?: Duration.Input;
  readonly flushInterval?: Duration.Input;
  readonly maxBatch?: number;
  readonly maxBuffer?: number;
  readonly requestTimeout?: Duration.Input;
  readonly secret: Redacted.Redacted;
  readonly url: string;
}

const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(IngestBatchSchema));

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]", "localhost"]);

/** The bearer secret must never travel in clear, so only https or loopback http is allowed. */
export const isSafeIngestUrl = (url: string) => {
  if (!URL.canParse(url)) {
    return false;
  }

  const { hostname, protocol } = new URL(url);

  return protocol === "https:" || (protocol === "http:" && LOOPBACK_HOSTS.has(hostname));
};

export const nodeEventSink = (options: NodeEventSinkOptions) =>
  Layer.effect(
    EventSink,
    Effect.gen(function* buildNodeEventSink() {
      if (!isSafeIngestUrl(options.url)) {
        yield* Effect.logWarning(
          "NodeEventSink refused its url and will send nothing: the bearer secret would travel in clear. Use an https:// ingest url, or http:// only for localhost, 127.0.0.1 or [::1].",
        );

        return { send: () => Effect.void };
      }

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

      // Chunks taken from the queue but not yet posted. Taking moves events here in one
      // uninterruptible step, so an interrupted drain loses at most the in-flight POST.
      const pending = yield* Ref.make<readonly (readonly RawEvent[])[]>([]);

      const take = Effect.uninterruptible(
        Queue.clear(queue).pipe(
          Effect.flatMap((events) =>
            Ref.update(pending, (chunks) => [...chunks, ...Arr.chunksOf(events, maxBatch)]),
          ),
        ),
      );

      const postPending: Effect.Effect<void> = Ref.modify(pending, ([next, ...rest]) => [
        next,
        rest,
      ]).pipe(
        Effect.flatMap((chunk) =>
          chunk === undefined ? Effect.void : post(chunk).pipe(Effect.andThen(postPending)),
        ),
      );

      const drain = take.pipe(Effect.andThen(postPending));
      const closed = yield* Ref.make(false);

      // Registered before the loop, so it runs after scope close has interrupted the loop.
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* closeSink() {
          yield* Ref.set(closed, true);
          yield* take;
          yield* Queue.shutdown(queue);
          yield* postPending;
        }).pipe(
          Effect.timeoutOrElse({
            duration: options.closeTimeout ?? DEFAULT_CLOSE_TIMEOUT,
            orElse: () =>
              Queue.shutdown(queue).pipe(
                Effect.andThen(
                  Effect.logWarning("event sink close timed out; unsent events were dropped"),
                ),
              ),
          }),
        ),
      );

      yield* Effect.forkScoped(
        Effect.forever(
          Effect.sleep(options.flushInterval ?? DEFAULT_FLUSH_INTERVAL).pipe(Effect.andThen(drain)),
        ),
      );

      return {
        // A send that races close offers to a shut-down queue, which rejects every event
        // without failing (pinned in node-sink.test.ts). catchCause is the backstop.
        send: (events: readonly RawEvent[]) =>
          Ref.get(closed).pipe(
            Effect.flatMap((isClosed) =>
              isClosed
                ? Effect.logWarning(`event sink is closed; dropped ${events.length} events`)
                : Queue.offerAll(queue, events).pipe(
                    Effect.flatMap((rejected) =>
                      rejected.length === 0
                        ? Effect.void
                        : Ref.get(closed).pipe(
                            Effect.flatMap((closedNow) =>
                              Effect.logWarning(
                                closedNow
                                  ? `event sink is closed; dropped ${rejected.length} events`
                                  : `event buffer full; dropped ${rejected.length} events`,
                              ),
                            ),
                          ),
                    ),
                    Effect.catchCause((cause) =>
                      Effect.logWarning(`event sink dropped ${events.length} events`, cause),
                    ),
                  ),
            ),
          ),
      };
    }),
  );

export const NodeEventSink = (options: NodeEventSinkOptions) =>
  nodeEventSink(options).pipe(Layer.provide(FetchHttpClient.layer));
