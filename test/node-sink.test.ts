import { describe, expect, it } from "@effect/vitest";
import { Clock, Effect, Exit, Layer, Logger, Redacted, Ref, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { TestClock } from "effect/testing";

import { EventSink } from "../src/index.js";
import type { RawEvent } from "../src/index.js";
import { IngestBatchSchema, ingestEvents } from "../src/ingest.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";
import { nodeEventSink } from "../src/node.js";
import type { NodeEventSinkOptions } from "../src/node.js";
import { SECRET, batchJson, rawEvent } from "./support.js";

const INGEST_URL = "https://app.example.test/_analytics/ingest";

interface Posted {
  readonly authorization: string | undefined;
  readonly events: readonly RawEvent[];
  readonly url: string;
}

const decodeBatch = Schema.decodeUnknownEffect(Schema.fromJsonString(IngestBatchSchema));

const bodyText = (request: HttpClientRequest.HttpClientRequest) =>
  request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "";

const recordingClient = (posted: Ref.Ref<readonly Posted[]>, status = 202) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request, url) =>
      decodeBatch(bodyText(request)).pipe(
        Effect.orDie,
        Effect.flatMap(({ events }) =>
          Ref.update(posted, (all) => [
            ...all,
            {
              authorization: request.headers.authorization,
              events,
              url: url.toString(),
            },
          ]),
        ),
        Effect.as(HttpClientResponse.fromWeb(request, new Response(null, { status }))),
      ),
    ),
  );

const TransportSchema = Schema.Literals(["refused", "status500", "hangs"]);

const brokenClient = (transport: typeof TransportSchema.Type) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) => {
      if (transport === "hangs") {
        return Effect.never;
      }

      return transport === "status500"
        ? Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 500 })))
        : Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                cause: new Error("connection refused"),
                request,
              }),
            }),
          );
    }),
  );

const options = (overrides: Partial<NodeEventSinkOptions> = {}) => ({
  flushInterval: "1 hour" as const,
  secret: SECRET,
  url: INGEST_URL,
  ...overrides,
});

const sendAll = (batches: readonly (readonly RawEvent[])[]) =>
  Effect.gen(function* sendBatches() {
    const sink = yield* EventSink;

    for (const batch of batches) {
      yield* sink.send(batch);
    }
  });

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum, minimum })));

describe("node event sink", () => {
  it.effect.prop(
    "a full buffer drops instead of blocking, and scope close flushes what fit in bounded batches",
    {
      batches: Arbitrary.array(Arbitrary.array(rawEvent, { maxLength: 4 }), {
        maxLength: 6,
      }),
      maxBatch: smallInt(1, 5),
      maxBuffer: smallInt(1, 10),
    },
    ({ batches, maxBatch, maxBuffer }) =>
      Effect.gen(function* bufferModel() {
        const posted = yield* Ref.make<readonly Posted[]>([]);

        const exit = yield* Effect.exit(
          sendAll(batches).pipe(
            Effect.provide(
              nodeEventSink(options({ maxBatch, maxBuffer })).pipe(
                Layer.provide(recordingClient(posted)),
              ),
            ),
          ),
        );

        const requests = yield* Ref.get(posted);
        const expected = batches.flat().slice(0, maxBuffer);

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(batchJson(requests.flatMap(({ events }) => events))).toBe(batchJson(expected));

        for (const request of requests) {
          expect(request.events.length).toBeGreaterThan(0);
          expect(request.events.length).toBeLessThanOrEqual(maxBatch);
          expect(request.authorization).toBe(`Bearer ${Redacted.value(SECRET)}`);
          expect(request.url).toBe(INGEST_URL);
        }
      }),
  );

  it.live.prop(
    "a dead ingest route never fails or blocks the caller, and scope close still finishes",
    {
      batches: Arbitrary.array(Arbitrary.array(rawEvent, { maxLength: 3 }), {
        maxLength: 4,
      }),
      transport: Arbitrary.schema(TransportSchema),
    },
    ({ batches, transport }) =>
      Effect.gen(function* deadTransport() {
        const exit = yield* Effect.exit(
          sendAll(batches).pipe(
            Effect.provide(
              nodeEventSink(options({ maxBatch: 2, requestTimeout: "20 millis" })).pipe(
                Layer.provide(brokenClient(transport)),
              ),
            ),
          ),
        );

        expect(Exit.isSuccess(exit)).toBe(true);
      }),
    { arbitrary: { runs: 20 } },
  );

  it.live.prop(
    "scope close against a hanging endpoint finishes within closeTimeout, mid-drain or not",
    {
      events: Arbitrary.array(rawEvent, { maxLength: 6, minLength: 1 }),
      midDrain: Arbitrary.schema(Schema.Boolean),
    },
    ({ events, midDrain }) =>
      Effect.gen(function* boundedClose() {
        const started = yield* Clock.currentTimeMillis;

        const exit = yield* Effect.exit(
          Effect.gen(function* sendThenWait() {
            yield* sendAll([events]);
            yield* Effect.sleep(midDrain ? "30 millis" : "0 millis");
          }).pipe(
            Effect.provide(
              nodeEventSink(
                options({
                  closeTimeout: "100 millis",
                  flushInterval: midDrain ? "5 millis" : "1 hour",
                  maxBatch: 1,
                  requestTimeout: "1 hour",
                }),
              ).pipe(Layer.provide(brokenClient("hangs"))),
            ),
            Effect.timeout("3 seconds"),
          ),
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect((yield* Clock.currentTimeMillis) - started).toBeLessThan(1000);
      }),
    { arbitrary: { runs: 6 } },
  );

  it.effect.prop(
    "send after scope close is a logged no-op that posts nothing",
    { events: Arbitrary.array(rawEvent, { maxLength: 4, minLength: 1 }) },
    ({ events }) =>
      Effect.gen(function* sendAfterClose() {
        const posted = yield* Ref.make<readonly Posted[]>([]);
        const logs: string[] = [];

        const sink = yield* Effect.scoped(
          Effect.gen(function* buildThenClose() {
            const context = yield* Layer.build(
              nodeEventSink(options()).pipe(Layer.provide(recordingClient(posted))),
            );

            return yield* EventSink.pipe(Effect.provideContext(context));
          }),
        );

        const exit = yield* Effect.exit(
          sink
            .send(events)
            .pipe(
              Effect.provide(Logger.layer([Logger.make((log) => logs.push(String(log.message)))])),
            ),
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(yield* Ref.get(posted)).toStrictEqual([]);
        expect(logs.some((line) => line.includes("closed"))).toBe(true);
      }),
  );

  it.effect("the background loop flushes on its interval without waiting for close", () =>
    Effect.gen(function* intervalFlush() {
      const posted = yield* Ref.make<readonly Posted[]>([]);
      const events = yield* Arbitrary.sampleEffect(rawEvent, { count: 3, seed: 5 }).pipe(
        Effect.orDie,
      );

      yield* Effect.scoped(
        Effect.gen(function* withinScope() {
          const context = yield* Layer.build(
            nodeEventSink(options({ flushInterval: "5 seconds" })).pipe(
              Layer.provide(recordingClient(posted)),
            ),
          );

          yield* sendAll([events]).pipe(Effect.provideContext(context));
          yield* TestClock.adjust("4 seconds");
          expect(yield* Ref.get(posted)).toStrictEqual([]);
          yield* TestClock.adjust("1 second");
          expect((yield* Ref.get(posted)).flatMap((request) => request.events)).toHaveLength(3);
        }),
      );

      expect(yield* Ref.get(posted)).toHaveLength(1);
    }),
  );

  it.effect.prop(
    "events sent from a process land in the Worker sink through the ingest route",
    {
      batches: Arbitrary.array(Arbitrary.array(rawEvent, { maxLength: 4 }), {
        maxLength: 4,
      }),
    },
    ({ batches }) =>
      Effect.gen(function* endToEnd() {
        const worker = yield* Layer.build(memoryEventsLayer("salt"));

        const throughIngest = Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request, url) =>
            ingestEvents({ maxEvents: 3, secret: SECRET }).pipe(
              Effect.provideService(
                HttpServerRequest.HttpServerRequest,
                HttpServerRequest.fromWeb(
                  new Request(url, {
                    body: bodyText(request),
                    headers: request.headers,
                    method: request.method,
                  }),
                ),
              ),
              Effect.provideContext(worker),
              Effect.map((response) =>
                HttpClientResponse.fromWeb(request, HttpServerResponse.toWeb(response)),
              ),
            ),
          ),
        );

        yield* sendAll(batches).pipe(
          Effect.provide(
            nodeEventSink(options({ maxBatch: 3 })).pipe(Layer.provide(throughIngest)),
          ),
        );

        const landed = yield* Effect.gen(function* readLanded() {
          return yield* (yield* EventSinkMemory).events;
        }).pipe(Effect.provideContext(worker));

        expect(batchJson(landed)).toBe(batchJson(batches.flat()));
      }),
  );
});
