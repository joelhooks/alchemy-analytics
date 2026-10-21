import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { EventSink, EventSinkError, RawEventSchema } from "../src/index.js";
import {
  DEFAULT_MAX_BODY_BYTES,
  DEFAULT_MAX_EVENTS,
  INGEST_PATH,
  ingestEvents,
  withIngestRoute,
} from "../src/ingest.js";
import type { IngestOptions } from "../src/ingest.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";
import { word } from "./arbitraries.js";
import { SECRET, batchJson, rawEvent, textChunks, trackedRequest } from "./support.js";
import type { PullTrackedBody } from "./support.js";

const URL_ = `https://app.example.test${INGEST_PATH}`;

const handle = (tracked: PullTrackedBody, options: Partial<IngestOptions> = {}) =>
  ingestEvents({ secret: SECRET, ...options }).pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, tracked.request),
  );

const recorded = Effect.gen(function* readRecorded() {
  return yield* (yield* EventSinkMemory).events;
});

const sampleEvents = (count: number, seed: number) =>
  Arbitrary.sampleEffect(rawEvent, { count, seed }).pipe(Effect.orDie);

const bearer = `Bearer ${Redacted.value(SECRET)}`;

const encodeEvent = Schema.encodeSync(RawEventSchema);

const WrongAuthSchema = Schema.Literals([
  "missing",
  "random",
  "prefix",
  "suffix",
  "basic",
  "lowercase",
  "empty",
]);

const authHeaderFor = (kind: typeof WrongAuthSchema.Type, noise: string) => {
  const secret = Redacted.value(SECRET);
  const cut = Math.max(0, secret.length - 1 - (noise.length % secret.length));

  const headers: Readonly<Record<typeof WrongAuthSchema.Type, string | null>> = {
    basic: `Basic ${secret}`,
    empty: "Bearer ",
    lowercase: `bearer ${secret}`,
    missing: null,
    prefix: `Bearer ${secret.slice(0, cut)}`,
    random: `Bearer ${noise}`,
    suffix: `Bearer ${secret}${noise}`,
  };

  return headers[kind];
};

describe("ingest route", () => {
  it.effect.prop(
    "an unauthenticated request gets 401 and its body is never pulled",
    {
      events: Arbitrary.array(rawEvent, { maxLength: 3 }),
      kind: Arbitrary.schema(WrongAuthSchema),
      noise: word,
    },
    ({ events, kind, noise }) =>
      Effect.gen(function* rejectUnauthenticated() {
        const header = authHeaderFor(kind, noise);

        const tracked = trackedRequest(
          URL_,
          header === null ? {} : { authorization: header },
          textChunks(batchJson(events), 64),
        );

        const response = yield* handle(tracked);

        expect(response.status).toBe(401);
        expect(response.headers["www-authenticate"]).toBe("Bearer");
        expect(tracked.wasPulled()).toBe(false);
        expect(yield* recorded).toStrictEqual([]);
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect("the right secret is accepted and the batch is written", () =>
    Effect.gen(function* acceptAuthenticated() {
      const events = yield* sampleEvents(3, 7);

      const tracked = trackedRequest(
        URL_,
        { authorization: bearer },
        textChunks(batchJson(events), 64),
      );

      const response = yield* handle(tracked);

      expect(response.status).toBe(202);
      expect(tracked.wasPulled()).toBe(true);
      expect(batchJson(yield* recorded)).toBe(batchJson(events));
    }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect.prop(
    "a declared Content-Length over the cap gets 413 before the body is pulled",
    {
      maxBodyBytes: Arbitrary.schema(
        Schema.Int.check(Schema.isBetween({ maximum: 4096, minimum: 1 })),
      ),
      over: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 10_000, minimum: 1 }))),
    },
    ({ maxBodyBytes, over }) =>
      Effect.gen(function* rejectDeclaredLength() {
        const tracked = trackedRequest(
          URL_,
          {
            authorization: bearer,
            "content-length": String(maxBodyBytes + over),
          },
          [new Uint8Array(maxBodyBytes + over)],
        );

        const response = yield* handle(tracked, { maxBodyBytes });

        expect(response.status).toBe(413);
        expect(tracked.wasPulled()).toBe(false);
        expect(yield* recorded).toStrictEqual([]);
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect.prop(
    "a streamed body over the cap gets 413 and is read no further than one chunk past the cap",
    {
      chunkSize: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 512, minimum: 1 }))),
      events: Arbitrary.array(rawEvent, { maxLength: 4, minLength: 1 }),
      lie: Arbitrary.schema(Schema.Boolean),
    },
    ({ chunkSize, events, lie }) =>
      Effect.gen(function* rejectStreamedLength() {
        const json = batchJson(events);
        const maxBodyBytes = Math.max(1, Math.floor(json.length / 2));
        const chunks = textChunks(json, chunkSize);

        const tracked = trackedRequest(
          URL_,
          lie ? { authorization: bearer, "content-length": "1" } : { authorization: bearer },
          [...chunks, ...chunks, ...chunks],
        );

        const response = yield* handle(tracked, { maxBodyBytes });

        expect(response.status).toBe(413);
        expect(tracked.pulledBytes()).toBeLessThanOrEqual(maxBodyBytes + chunkSize);
        expect(yield* recorded).toStrictEqual([]);
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect.prop(
    "a batch over the event bound gets 413 and none of it is written; within the bound all of it is",
    {
      events: Arbitrary.array(rawEvent, { maxLength: 12 }),
      maxEvents: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 8, minimum: 1 }))),
    },
    ({ events, maxEvents }) =>
      Effect.gen(function* boundBatch() {
        const tracked = trackedRequest(
          URL_,
          { authorization: bearer },
          textChunks(batchJson(events), 256),
        );

        const response = yield* handle(tracked, { maxEvents });
        const written = yield* recorded;

        if (events.length > maxEvents) {
          expect(response.status).toBe(413);
          expect(written).toStrictEqual([]);
        } else {
          expect(response.status).toBe(202);
          expect(batchJson(written)).toBe(batchJson(events));
        }
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect.prop(
    "one invalid event rejects the whole batch with 400",
    {
      events: Arbitrary.array(rawEvent, { maxLength: 5, minLength: 1 }),
      position: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 5, minimum: 0 }))),
    },
    ({ events, position }) =>
      Effect.gen(function* rejectInvalid() {
        const valid: unknown[] = events.map((event) => encodeEvent(event));
        valid.splice(position, 0, { v: 2 });

        const tracked = trackedRequest(
          URL_,
          { authorization: bearer },
          textChunks(JSON.stringify({ events: valid }), 128),
        );

        const response = yield* handle(tracked);

        expect(response.status).toBe(400);
        expect(yield* recorded).toStrictEqual([]);
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect(
    "malformed JSON, wrong methods and a failing or dying sink answer without throwing",
    () =>
      Effect.gen(function* edgeAnswers() {
        const malformed = yield* handle(
          trackedRequest(URL_, { authorization: bearer }, textChunks("{nope", 4)),
        ).pipe(Effect.provide(memoryEventsLayer("salt")));

        const wrongMethod = trackedRequest(URL_, { authorization: bearer }, [], "GET");
        const wrongMethodResponse = yield* handle(wrongMethod).pipe(
          Effect.provide(memoryEventsLayer("salt")),
        );

        const sinkDown = yield* handle(
          trackedRequest(
            URL_,
            { authorization: bearer },
            textChunks(batchJson(yield* sampleEvents(2, 3)), 64),
          ),
        ).pipe(
          Effect.provide(
            Layer.succeed(EventSink, {
              send: () => Effect.fail(new EventSinkError({ cause: new Error("down") })),
            }),
          ),
        );

        const sinkDies = yield* handle(
          trackedRequest(
            URL_,
            { authorization: bearer },
            textChunks(batchJson(yield* sampleEvents(2, 3)), 64),
          ),
        ).pipe(
          Effect.provide(Layer.succeed(EventSink, { send: () => Effect.die(new Error("threw")) })),
        );

        expect(malformed.status).toBe(400);
        expect(sinkDies.status).toBe(502);
        expect(wrongMethodResponse.status).toBe(405);
        expect(wrongMethod.wasPulled()).toBe(false);
        expect(sinkDown.status).toBe(502);
      }),
  );

  it.effect.prop(
    "a Content-Length that is not plain decimal is ignored, and the streamed cap decides",
    {
      header: Arbitrary.schema(Schema.Literals(["1e9", "0x40000000", "2e7", "1.0e8", "+9e9"])),
    },
    ({ header }) =>
      Effect.gen(function* nonDecimalLength() {
        const events = yield* sampleEvents(2, 7);

        const response = yield* handle(
          trackedRequest(
            URL_,
            { authorization: bearer, "content-length": header },
            textChunks(batchJson(events), 64),
          ),
        );

        expect(response.status).toBe(202);
        expect(batchJson(yield* recorded)).toBe(batchJson(events));
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect("the default limits reject one event and one byte past them", () =>
    Effect.gen(function* defaultLimits() {
      const [event] = yield* sampleEvents(1, 11);
      const tooMany = Array.from({ length: DEFAULT_MAX_EVENTS + 1 }, () => event);

      const overBound = yield* handle(
        trackedRequest(
          URL_,
          { authorization: bearer },
          textChunks(batchJson(tooMany.filter((item) => item !== undefined)), 4096),
        ),
      );

      const overBytes = trackedRequest(
        URL_,
        {
          authorization: bearer,
          "content-length": String(DEFAULT_MAX_BODY_BYTES + 1),
        },
        [new Uint8Array(DEFAULT_MAX_BODY_BYTES + 1)],
      );

      const overBytesResponse = yield* handle(overBytes);

      expect(overBound.status).toBe(413);
      expect(overBytesResponse.status).toBe(413);
      expect(overBytes.wasPulled()).toBe(false);
      expect(yield* recorded).toStrictEqual([]);
    }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );

  it.effect.prop(
    "withIngestRoute answers only its own path and passes everything else to the app",
    { onIngest: Arbitrary.schema(Schema.Boolean), path: word },
    ({ onIngest, path }) =>
      Effect.gen(function* route() {
        const tracked = trackedRequest(
          onIngest ? URL_ : `https://app.example.test/${path}`,
          {},
          [],
        );

        const app = withIngestRoute({ secret: SECRET })(
          Effect.succeed(HttpServerResponse.text("app", { status: 299 })),
        );

        const response = yield* app.pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, tracked.request),
        );

        const malformed = yield* app.pipe(
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            tracked.request.modify({ url: "//" }),
          ),
        );

        expect(response.status).toBe(onIngest ? 401 : 299);
        expect(malformed.status).toBe(299);
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );
});
