import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { VISITOR_COOKIE, captureEvent, withEventCapture } from "../src/index.js";
import { memoryEventsLayer } from "../src/memory.js";
import { queryPairs, word } from "./arbitraries.js";
import { SinkFailureSchema, brokenSink } from "./support.js";

const Scenario = Schema.Struct({
  binary: Schema.Boolean,
  html: Schema.Boolean,
  method: Schema.Literals(["GET", "POST", "PUT", "DELETE"]),
  mode: Schema.Literals(["daily", "persistent"]),
  status: Schema.Literals([200, 201, 204, 302, 404, 500, 503]),
  withCookie: Schema.Boolean,
});

const exchange = Arbitrary.all({
  bytes: Arbitrary.array(
    Arbitrary.schema(Schema.Int.check(Schema.isBetween({ maximum: 255, minimum: 0 }))),
    { maxLength: 64 },
  ),
  failure: Arbitrary.schema(SinkFailureSchema),
  path: Arbitrary.array(word, { maxLength: 3 }),
  query: queryPairs,
  scenario: Arbitrary.schema(Scenario),
  text: word,
});

type Generated = typeof exchange extends Arbitrary.Arbitrary<infer A> ? A : never;

const requestFor = (generated: Generated) => {
  const query = new URLSearchParams(generated.query).toString();

  const headers = new Headers({
    accept: generated.scenario.html ? "text/html" : "application/json",
    "cf-connecting-ip": "203.0.113.9",
    "user-agent": "model-test",
  });

  if (generated.scenario.withCookie) {
    headers.set("cookie", `${VISITOR_COOKIE}=0199a3b2-6f1e-7c3d-8a4b-1c2d3e4f5a6b`);
  }

  return HttpServerRequest.fromWeb(
    new Request(
      `https://app.example.test/${generated.path.join("/")}${query === "" ? "" : `?${query}`}`,
      { headers, method: generated.scenario.method },
    ),
  );
};

const responseFor = (generated: Generated) => {
  const { binary, html, status } = generated.scenario;

  return binary
    ? HttpServerResponse.uint8Array(Uint8Array.from(generated.bytes), {
        contentType: "application/octet-stream",
        headers: { "x-app": generated.text },
        status,
      })
    : HttpServerResponse.text(generated.text, {
        contentType: html ? "text/html; charset=utf-8" : "text/plain",
        headers: { "x-app": generated.text },
        status,
      });
};

const snapshot = (response: HttpServerResponse.HttpServerResponse) =>
  Effect.gen(function* snapshotResponse() {
    const web = HttpServerResponse.toWeb(response);

    const body = new Uint8Array(
      // oxlint-disable-next-line typescript/promise-function-async -- the web Response owns this Promise-returning boundary.
      yield* Effect.promise(() => web.arrayBuffer()),
    );

    const headers = [...web.headers].filter(
      ([name, value]) => !(name === "set-cookie" && value.startsWith(`${VISITOR_COOKIE}=`)),
    );

    return {
      body: [...body],
      headers,
      status: web.status,
      visitorCookie: web.headers
        .getSetCookie()
        .some((cookie) => cookie.startsWith(`${VISITOR_COOKIE}=`)),
    };
  });

const captured = (generated: Generated) =>
  withEventCapture({
    identityMode: generated.scenario.mode,
    runInBackground: (effect) => effect,
  })(Effect.succeed(responseFor(generated))).pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, requestFor(generated)),
  );

describe("capture first", () => {
  it.effect.prop(
    "a broken sink or salt never changes the status, headers or body bytes, and the caller still succeeds",
    { generated: exchange },
    ({ generated }) =>
      Effect.gen(function* brokenSinkModel() {
        const bare = yield* snapshot(responseFor(generated));

        const healthy = yield* snapshot(
          yield* captured(generated).pipe(Effect.provide(memoryEventsLayer("salt"))),
        );

        const exit = yield* Effect.exit(
          captured(generated).pipe(Effect.provide(brokenSink(generated.failure))),
        );

        expect(Exit.isSuccess(exit)).toBe(true);

        if (Exit.isSuccess(exit)) {
          const broken = yield* snapshot(exit.value);

          expect(broken.status).toBe(bare.status);
          expect(broken.body).toStrictEqual(bare.body);
          expect(broken.headers).toStrictEqual(bare.headers);
          expect(healthy.body).toStrictEqual(bare.body);
          expect(healthy.headers).toStrictEqual(bare.headers);
          expect(broken.visitorCookie && !healthy.visitorCookie).toBe(false);
        }
      }),
    { arbitrary: { runs: 200 } },
  );

  it.effect.prop(
    "an app failure passes through capture untouched",
    { failure: Arbitrary.schema(SinkFailureSchema), generated: exchange, reason: word },
    ({ failure, generated, reason }) =>
      Effect.gen(function* appFailure() {
        const exit = yield* Effect.exit(
          withEventCapture({
            identityMode: generated.scenario.mode,
            runInBackground: (effect) => effect,
          })(Effect.fail({ reason })).pipe(
            Effect.provideService(HttpServerRequest.HttpServerRequest, requestFor(generated)),
            Effect.provide(brokenSink(failure)),
          ),
        );

        expect(exit).toStrictEqual(Exit.fail({ reason }));
      }),
  );

  it.effect.prop(
    "captureEvent always succeeds for its caller, whatever the sink does",
    {
      body: Arbitrary.schema(Schema.Json),
      failure: Arbitrary.schema(SinkFailureSchema),
      host: word,
    },
    ({ body, failure, host }) =>
      Effect.gen(function* callerSucceeds() {
        const exit = yield* Effect.exit(
          captureEvent({
            anonymousId: "installation-1",
            body: Schema.Json,
            host,
          })(body).pipe(Effect.provide(brokenSink(failure))),
        );

        expect(Exit.isSuccess(exit)).toBe(true);
      }),
  );
});
