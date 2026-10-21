import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import { uuidV7Of } from "../src/web-crypto.js";

const MAX_TIMESTAMP = 2 ** 48 - 1;

const Millis = Schema.Int.check(Schema.isBetween({ maximum: MAX_TIMESTAMP, minimum: 0 }));

const RandomBytes = Schema.Array(Schema.Int.check(Schema.isBetween({ maximum: 255, minimum: 0 })))
  .check(Schema.isMinLength(16))
  .check(Schema.isMaxLength(16));

const timestampOf = (uuid: string) => Number.parseInt(uuid.replaceAll("-", "").slice(0, 12), 16);

describe("uuidV7Of", () => {
  it.effect.prop(
    "lays out version 7, variant 10 and the timestamp for any input",
    { millis: Millis, random: RandomBytes },
    ({ millis, random }) =>
      Effect.sync(() => {
        const uuid = uuidV7Of(millis, Uint8Array.from(random));

        expect(uuid).toMatch(
          /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
        );
        expect(timestampOf(uuid)).toBe(millis);
      }),
    { arbitrary: { runs: 200 } },
  );

  it.effect.prop(
    "sorts by timestamp as a string",
    { a: Millis, b: Millis, x: RandomBytes, y: RandomBytes },
    ({ a, b, x, y }) =>
      Effect.sync(() => {
        const first = uuidV7Of(a, Uint8Array.from(x));
        const second = uuidV7Of(b, Uint8Array.from(y));

        if (a !== b) {
          expect(a < b).toBe(first < second);
        }
      }),
    { arbitrary: { runs: 200 } },
  );

  it("matches the RFC 9562 layout for known bytes", () => {
    expect(uuidV7Of(0x01_7f_22_e2_79_b0, new Uint8Array(16).fill(0xff))).toBe(
      "017f22e2-79b0-7fff-bfff-ffffffffffff",
    );
  });
});
