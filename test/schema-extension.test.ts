import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";

import {
  RawEventSchema,
  captureEvent,
  fromRawEvent,
  rawEventSchema,
  toRawEvent,
} from "../src/index.js";
import type { RawEventOf } from "../src/index.js";
import { IngestBatchSchema } from "../src/ingest.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";

const SignupBody = Schema.Struct({
  at: Schema.DateTimeUtcFromString,
  plan: Schema.Literals(["free", "pro", "team"]),
  ratio: Schema.FiniteFromString,
  seats: Schema.Int,
  tags: Schema.Array(Schema.String),
  trial: Schema.optionalKey(Schema.Boolean),
  type: Schema.Literal("signup"),
});

const SignupEvent = rawEventSchema(SignupBody);

type Signup = RawEventOf<typeof SignupBody>;

const wire = Schema.fromJsonString(IngestBatchSchema);

const sameEvent = Schema.toEquivalence(SignupEvent);

const sameBody = Schema.toEquivalence(SignupBody);

const viaWire = (event: Signup) =>
  Effect.gen(function* sendOverWire() {
    const raw = yield* toRawEvent(SignupEvent)(event);
    const json = yield* Schema.encodeEffect(wire)({ events: [raw] });
    const { events } = yield* Schema.decodeUnknownEffect(wire)(json);

    const decode = fromRawEvent(SignupEvent);

    return yield* Effect.all(events.map((item) => decode(item)));
  });

describe("extended event schemas", () => {
  it.effect.prop(
    "an event with a project body schema round-trips through the base event and the ingest wire",
    { event: Arbitrary.schema(SignupEvent) },
    ({ event }) =>
      Effect.gen(function* roundTrip() {
        const [back] = yield* viaWire(event);

        expect(back !== undefined && sameEvent(back, event)).toBe(true);
      }),
  );

  it.effect.prop(
    "captureEvent with a project body schema records a base event that decodes back to the same body",
    { body: Arbitrary.schema(SignupBody) },
    ({ body }) =>
      Effect.gen(function* recordTyped() {
        yield* captureEvent({
          anonymousId: "installation-7",
          body: SignupBody,
          host: "cli.example.test",
        })(body);

        const [event] = yield* (yield* EventSinkMemory).events;

        expect(event).toBeDefined();

        if (event !== undefined) {
          expect(Schema.is(RawEventSchema)(event)).toBe(true);
          expect(event.source).toBe("node");
          expect(sameBody((yield* fromRawEvent(SignupEvent)(event)).body, body)).toBe(true);
        }
      }).pipe(Effect.provide(memoryEventsLayer("salt"))),
  );
});
