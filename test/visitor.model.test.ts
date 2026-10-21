import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { TestClock } from "effect/testing";

import { withEventCapture } from "../src/index.js";
import type { IdentityMode, RawEvent } from "../src/index.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";
import { resolveVisitor, saltedHash } from "../src/visitor.js";
import { webCryptoLayer } from "../src/web-crypto.js";

const Visit = Schema.Struct({
  device: Schema.Literals([0, 1, 2]),
  keepsCookie: Schema.Boolean,
  startsNewDay: Schema.Boolean,
});

const commands = Arbitrary.array(Arbitrary.schema(Visit), { maxLength: 30 });

const devices = [
  { ip: "203.0.113.1", userAgent: "Mozilla/5.0 test" },
  { ip: "203.0.113.2", userAgent: "Mozilla/5.0 test" },
  { ip: "198.51.100.9", userAgent: "curl/8" },
] as const;

const dayOf = (offset: number) => `2026-10-${String(1 + offset).padStart(2, "0")}`;

const runSession = (mode: IdentityMode, steps: readonly (typeof Visit.Type)[]) =>
  Effect.gen(function* replaySession() {
    let day = 0;
    const cookies = new Map<number, string>();
    const seen = new Map<string, string>();
    const idsByDay = new Map<number, Set<string>>();

    for (const step of steps) {
      if (step.startsNewDay) {
        day += 1;
      }

      const device = devices[step.device];
      const cookie = step.keepsCookie ? cookies.get(step.device) : undefined;

      const visitor = yield* resolveVisitor({
        cookie,
        day: dayOf(day),
        ip: device.ip,
        mode,
        salt: "model-salt",
        userAgent: device.userAgent,
      });

      if (mode === "persistent") {
        expect(visitor.issueCookie).toBe(cookie === undefined);

        if (cookie !== undefined) {
          expect(visitor.anonymousId).toBe(cookie);
        }

        cookies.set(step.device, visitor.anonymousId);
        continue;
      }

      expect(visitor.issueCookie).toBe(false);
      const key = `${day}|${device.ip}|${device.userAgent}`;
      const previous = seen.get(key);

      if (previous !== undefined) {
        expect(visitor.anonymousId).toBe(previous);
      }

      seen.set(key, visitor.anonymousId);
      const today = idsByDay.get(day) ?? new Set<string>();
      today.add(visitor.anonymousId);
      idsByDay.set(day, today);
    }

    const days = [...idsByDay.values()];

    const repeated = days.flatMap((ids, index) =>
      days.slice(index + 1).flatMap((later) => [...ids].filter((id) => later.has(id))),
    );

    expect(repeated).toStrictEqual([]);
  }).pipe(Effect.provide(webCryptoLayer));

const DAY_MS = 86_400_000;

const twoDays = Arbitrary.all({
  device: Arbitrary.schema(Schema.Literals([0, 1, 2])),
  firstDay: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 30_000, minimum: 0 }))),
  laterDays: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 400, minimum: 1 }))),
  offsets: Arbitrary.array(
    Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: DAY_MS - 1, minimum: 0 }))),
    { maxLength: 2, minLength: 2 },
  ),
});

const SALT = "permanent-salt";

const identifiersOf = (event: RawEvent) => [event.anonymousId, event.server.ipHash ?? ""];

const visitAt = (time: number, device: (typeof devices)[number]) =>
  Effect.gen(function* visit() {
    yield* TestClock.setTime(time);

    yield* withEventCapture({ identityMode: "daily", runInBackground: (effect) => effect })(
      Effect.succeed(HttpServerResponse.text("ok")),
    ).pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(
          new Request("https://app.example.test/", {
            headers: { "cf-connecting-ip": device.ip, "user-agent": device.userAgent },
          }),
        ),
      ),
    );
  });

describe("visitor identity model", () => {
  it.effect.prop(
    "a daily visitor shares every identifier within a UTC day and none across days or with the permanent salt",
    { visits: twoDays },
    ({ visits }) =>
      Effect.gen(function* dailyRotation() {
        const device = devices[visits.device];
        const [early = 0, late = 0] = visits.offsets;
        const dayStart = visits.firstDay * DAY_MS;

        yield* visitAt(dayStart + early, device);
        yield* visitAt(dayStart + late, device);
        yield* visitAt(dayStart + visits.laterDays * DAY_MS + early, device);

        const [first, second, later] = (yield* (yield* EventSinkMemory).events).map(identifiersOf);

        const permanent = yield* saltedHash(SALT, ["ip", device.ip]).pipe(
          Effect.provide(webCryptoLayer),
        );

        expect(second).toStrictEqual(first);
        expect(first?.[1]).not.toBe("");

        for (const id of later ?? []) {
          expect(first).not.toContain(id);
        }

        expect([...(first ?? []), ...(later ?? [])]).not.toContain(permanent);
      }).pipe(Effect.provide(memoryEventsLayer(SALT))),
  );

  it.effect.prop(
    "persistent ids survive every visit that keeps the cookie and are reissued when it is lost",
    { steps: commands },
    ({ steps }) => runSession("persistent", steps),
  );

  it.effect.prop(
    "daily ids are stable within a day, never set a cookie, and never repeat across days",
    { steps: commands },
    ({ steps }) => runSession("daily", steps),
  );
});
