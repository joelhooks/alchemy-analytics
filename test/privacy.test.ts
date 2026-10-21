import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { RawEventSchema, VISITOR_COOKIE, isSensitiveKey, withEventCapture } from "../src/index.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";
import { queryKey, word } from "./arbitraries.js";

const hex = Arbitrary.schema(
  Schema.Literals(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]),
);

const secret = Arbitrary.array(hex, { maxLength: 16, minLength: 16 }).pipe(
  Arbitrary.map((digits) => `zq${digits.join("")}`),
);

const Shape = Schema.Struct({
  bodyKind: Schema.Literals(["json", "form", "text"]),
  captureQuery: Schema.Boolean,
  html: Schema.Boolean,
  mode: Schema.Literals(["daily", "persistent"]),
  visitorCookie: Schema.Boolean,
});

const leakyRequest = Arbitrary.all({
  apiKey: secret,
  authorization: secret,
  bodySecret: secret,
  cookieSecret: secret,
  email: secret.pipe(Arbitrary.map((local) => `${local}@mail.example.test`)),
  password: secret,
  path: word,
  queryKeys: Arbitrary.array(queryKey, { maxLength: 5 }),
  querySecrets: Arbitrary.array(secret, { maxLength: 5, minLength: 5 }),
  referrerSecret: secret,
  responseSecret: secret,
  shape: Arbitrary.schema(Shape),
});

type Leaky = typeof leakyRequest extends Arbitrary.Arbitrary<infer A> ? A : never;

const bodyOf = (leaky: Leaky) => {
  const fields = {
    email: leaky.email,
    note: leaky.bodySecret,
    password: leaky.password,
  };

  const bodies = {
    form: {
      body: new URLSearchParams(fields).toString(),
      type: "application/x-www-form-urlencoded",
    },
    json: { body: JSON.stringify(fields), type: "application/json" },
    text: {
      body: `${leaky.email} ${leaky.bodySecret} ${leaky.password}`,
      type: "text/plain",
    },
  };

  return bodies[leaky.shape.bodyKind];
};

const queryOf = (leaky: Leaky) =>
  leaky.queryKeys.map((key, index): [string, string] => [
    key,
    leaky.querySecrets[index] ?? leaky.bodySecret,
  ]);

const requestOf = (leaky: Leaky) => {
  const { body, type } = bodyOf(leaky);
  const query = new URLSearchParams([
    ...queryOf(leaky),
    ["email", leaky.email],
    ["token", leaky.password],
  ]);

  const cookies = [
    `session=${leaky.cookieSecret}`,
    `auth=${leaky.authorization}`,
    ...(leaky.shape.visitorCookie
      ? [`${VISITOR_COOKIE}=0199a3b2-6f1e-7c3d-8a4b-1c2d3e4f5a6b`]
      : []),
  ];

  return HttpServerRequest.fromWeb(
    new Request(`https://app.example.test/${leaky.path}?${query.toString()}`, {
      body,
      headers: {
        accept: leaky.shape.html ? "text/html" : "application/json",
        authorization: `Bearer ${leaky.authorization}`,
        "cf-connecting-ip": "198.51.100.23",
        "content-type": type,
        cookie: cookies.join("; "),
        "proxy-authorization": `Basic ${leaky.authorization}`,
        referer: `https://ref.example.test/page?code=${leaky.referrerSecret}#${leaky.referrerSecret}`,
        "x-api-key": leaky.apiKey,
      },
      method: "POST",
    }),
  );
};

const encodeEvent = Schema.encodeEffect(RawEventSchema);

describe("privacy", () => {
  it.effect.prop(
    "bodies, emails, cookies, auth headers, credentials and query values never reach an event",
    { leaky: leakyRequest },
    ({ leaky }) =>
      Effect.gen(function* neverLeak() {
        yield* withEventCapture({
          captureQuery: leaky.shape.captureQuery,
          identityMode: leaky.shape.mode,
          runInBackground: (effect) => effect,
        })(
          Effect.succeed(
            HttpServerResponse.text(`private ${leaky.responseSecret}`, {
              contentType: leaky.shape.html ? "text/html" : "application/json",
              headers: { "x-response-token": leaky.responseSecret },
            }),
          ),
        ).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, requestOf(leaky)));

        const events = yield* (yield* EventSinkMemory).events;

        expect(events).toHaveLength(1);

        const encoded = JSON.stringify(
          yield* Effect.all(events.map((event) => encodeEvent(event))),
        );

        const neverCaptured = [
          leaky.apiKey,
          leaky.authorization,
          leaky.bodySecret,
          leaky.cookieSecret,
          leaky.email,
          leaky.password,
          leaky.referrerSecret,
          leaky.responseSecret,
          "198.51.100.23",
        ];

        for (const value of neverCaptured) {
          expect(encoded).not.toContain(value);
        }

        for (const [key, value] of queryOf(leaky)) {
          const allowed = leaky.shape.captureQuery && !isSensitiveKey(key);

          if (!allowed) {
            expect(encoded).not.toContain(value);
          }
        }
      }).pipe(Effect.provide(memoryEventsLayer("privacy-salt"))),
    { arbitrary: { runs: 300 } },
  );
});
