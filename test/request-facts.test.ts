import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";

import {
  MAX_QUERY_KEYS,
  MAX_QUERY_VALUES_PER_KEY,
  MAX_QUERY_VALUE_LENGTH,
  capturedQuery,
  redactIdSegments,
  referrerOf,
} from "../src/request-facts.js";
import { idSegment, queryPairs, slug, word } from "./arbitraries.js";

const pathParts = Arbitrary.array(
  Arbitrary.flatMap(Arbitrary.schema(Schema.Boolean), (isId) =>
    isId ? idSegment : slug.pipe(Arbitrary.map((value) => ({ marker: value, value }))),
  ),
  { maxLength: 6 },
);

const keyIndex = Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 39, minimum: 0 })));

const PASS_THROUGH = [
  "/about",
  "/robots.txt",
  "/v2/api",
  "/api/v1/members/42/posts",
  "/members/123",
  "/products/iphone-15-pro-max",
  "/blog/2026/hello",
  "/blog/2026/10/21/post",
  "/archive/2024",
  "/archive/1999",
  "/2026-10-hello-world",
  "/changelog/2026-q3",
  "/docs/es2015",
  "/css/main.1234.css",
  "/img/hero-1920x1080.png",
  "/assets/index-3f9c0a6e1b2d4c5e.js",
  "/releases/v1.2.3456",
  "/internationalization2",
  "/blog/how-to-use-effect",
] as const;

const REDACTED = [
  ["/orders/1234567", "/orders/:id"],
  ["/orders/1234", "/orders/:id"],
  ["/orders/2100", "/orders/:id"],
  ["/members/ada@example.com/orders/40213", "/members/:email/orders/:id"],
  ["/members/ada%40example.com", "/members/:email"],
  ["/runs/0199a3b2-6f1e-7c3d-8a4b-1c2d3e4f5a6b", "/runs/:id"],
  ["/reset/3f9c0a6e1b2d4c5e8f7a", "/reset/:id"],
  ["/blobs/da39a3ee5e6b4b0d3255bfef95601890afd80709", "/blobs/:id"],
  ["/invite/dGhpc0lzQVRva2VuMTIz", "/invite/:id"],
  ["/share/k3j4h5g6f7d8s9a0q1w2", "/share/:id"],
  ["/magic/Xy7_kQ2-pL9mN4vB8cR1", "/magic/:id"],
] as const;

describe("request facts", () => {
  it("ordinary, dated, versioned and file-name segments pass through", () => {
    expect(PASS_THROUGH.map((path) => redactIdSegments(path))).toStrictEqual([...PASS_THROUGH]);
  });

  it("emails, numeric ids, UUIDs and opaque tokens are redacted", () => {
    expect(REDACTED.map(([path]) => redactIdSegments(path))).toStrictEqual(
      REDACTED.map(([, redacted]) => redacted),
    );
  });

  it.prop(
    "captured queries keep only allow-listed keys, within the key, value and length caps",
    {
      extra: Arbitrary.array(word, { maxLength: 5 }),
      flags: Arbitrary.array(Arbitrary.schema(Schema.Boolean), { maxLength: 40, minLength: 40 }),
      long: Arbitrary.schema(Schema.Boolean),
      pairs: Arbitrary.array(Arbitrary.all([keyIndex, word]), { maxLength: 100, minLength: 70 }),
    },
    ({ extra, flags, long, pairs }) => {
      const padded = pairs.map(([index, value]): [string, string] => [
        `k${index}`,
        long ? value.padEnd(MAX_QUERY_VALUE_LENGTH + 50, "x") : value,
      ]);

      const allowed = [
        ...flags.flatMap((allow, index) => (allow || index % 4 !== 0 ? [`k${index}`] : [])),
        ...extra,
      ];

      const captured = capturedQuery(new URLSearchParams(padded), allowed);
      const keys = [...new Set(padded.map(([key]) => key))].filter((key) => allowed.includes(key));

      expect(Object.keys(captured).toSorted()).toStrictEqual(
        keys.slice(0, MAX_QUERY_KEYS).toSorted(),
      );

      for (const [key, values] of Object.entries(captured)) {
        expect(values).toStrictEqual(
          padded
            .filter(([candidate]) => candidate === key)
            .map(([, value]) => value.slice(0, MAX_QUERY_VALUE_LENGTH))
            .slice(0, MAX_QUERY_VALUES_PER_KEY),
        );
      }
    },
  );

  it.prop(
    "a repeated allow-listed key keeps only its first values, up to the per-key cap",
    {
      values: Arbitrary.array(word, {
        maxLength: MAX_QUERY_VALUES_PER_KEY * 3,
        minLength: MAX_QUERY_VALUES_PER_KEY + 1,
      }),
    },
    ({ values }) => {
      const params = new URLSearchParams(values.map((value): [string, string] => ["q", value]));

      expect(capturedQuery(params, ["q"])).toStrictEqual({
        q: values.slice(0, MAX_QUERY_VALUES_PER_KEY),
      });
    },
  );

  it.prop("an empty allow-list records no query at all", { pairs: queryPairs }, ({ pairs }) => {
    expect(capturedQuery(new URLSearchParams(pairs), [])).toStrictEqual({});
  });

  it.prop(
    "id-like path segments become :id or :email and every other segment survives",
    { parts: pathParts },
    ({ parts }) => {
      const path = `/${parts.map(({ value }) => encodeURIComponent(value)).join("/")}`;

      expect(redactIdSegments(path)).toBe(`/${parts.map(({ marker }) => marker).join("/")}`);
    },
  );

  it.prop(
    "referrers never carry a query string or fragment, and their paths are redacted",
    {
      fragment: word,
      parts: pathParts,
      query: word,
      secure: Arbitrary.schema(Schema.Boolean),
    },
    ({ fragment, parts, query, secure }) => {
      const origin = `${secure ? "https" : "http"}://example.test`;
      const path = parts.map(({ value }) => encodeURIComponent(value)).join("/");

      expect(referrerOf(`${origin}/${path}?q=${query}#${fragment}`)).toBe(
        `${origin}/${parts.map(({ marker }) => marker).join("/")}`,
      );
      expect(referrerOf(path)).toBeUndefined();
    },
  );
});
