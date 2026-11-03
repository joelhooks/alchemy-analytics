import { readFileSync } from "node:fs";

import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import { identitySalt, resolveVisitor, saltedHash } from "../src/visitor.js";
import { webCryptoLayer } from "../src/web-crypto.js";

const Hex = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u));

const Fixture = Schema.Struct({
  dailyIpHashes: Schema.Array(
    Schema.Struct({ day: Schema.String, hex: Hex, ip: Schema.String, salt: Schema.String }),
  ),
  dailySalts: Schema.Array(Schema.Struct({ day: Schema.String, hex: Hex, salt: Schema.String })),
  dailyVisitors: Schema.Array(
    Schema.Struct({
      anonymousId: Hex,
      day: Schema.String,
      ip: Schema.NullOr(Schema.String),
      salt: Schema.String,
      userAgent: Schema.NullOr(Schema.String),
    }),
  ),
  salted: Schema.Array(
    Schema.Struct({ hex: Hex, parts: Schema.Array(Schema.String), salt: Schema.String }),
  ),
});

const fixture = Schema.decodeSync(Schema.fromJsonString(Fixture))(
  readFileSync(new URL("fixtures/visitor-digests-4.0.json", import.meta.url), "utf-8"),
);

describe("visitor digests captured on Effect 4.0.x", () => {
  it.effect("saltedHash reproduces every captured digest", () =>
    Effect.gen(function* replaySalted() {
      for (const { hex, parts, salt } of fixture.salted) {
        expect(yield* saltedHash(salt, parts)).toBe(hex);
      }
    }).pipe(Effect.provide(webCryptoLayer)),
  );

  it.effect("the daily identity salt reproduces every captured salt", () =>
    Effect.gen(function* replayDailySalts() {
      for (const { day, hex, salt } of fixture.dailySalts) {
        expect(yield* identitySalt(salt, "daily", day)).toBe(hex);
      }
    }).pipe(Effect.provide(webCryptoLayer)),
  );

  it.effect("a daily visitor keeps every captured anonymous id", () =>
    Effect.gen(function* replayDailyVisitors() {
      for (const { anonymousId, day, ip, salt, userAgent } of fixture.dailyVisitors) {
        const visitor = yield* resolveVisitor({
          cookie: undefined,
          day,
          ip: ip ?? undefined,
          mode: "daily",
          salt,
          userAgent: userAgent ?? undefined,
        });

        expect(visitor).toStrictEqual({ anonymousId, issueCookie: false });
      }
    }).pipe(Effect.provide(webCryptoLayer)),
  );

  it.effect("a daily ip hash keeps every captured value", () =>
    Effect.gen(function* replayDailyIpHashes() {
      for (const { day, hex, ip, salt } of fixture.dailyIpHashes) {
        const idSalt = yield* identitySalt(salt, "daily", day);

        expect(yield* saltedHash(idSalt, ["ip", ip])).toBe(hex);
      }
    }).pipe(Effect.provide(webCryptoLayer)),
  );
});
