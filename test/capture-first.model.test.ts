import { describe, expect, it } from "@effect/vitest";
import { Clock, Effect, Exit, Layer, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import {
  EventSink,
  VISITOR_COOKIE,
  VisitorSalt,
  captureEvent,
  withEventCapture,
} from "../src/index.js";
import { memoryEventsLayer } from "../src/memory.js";
import { queryPairs, word } from "./arbitraries.js";
import { SinkFailureSchema, brokenSink, withBackground } from "./support.js";

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
  withBackground((runInBackground) =>
    withEventCapture({ identityMode: generated.scenario.mode, runInBackground })(
      Effect.succeed(responseFor(generated)),
    ).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, requestFor(generated))),
  );

const HangSchema = Schema.Literals(["salt", "sinkAwaited", "sinkDeferred"]);

const hungLayer = (hang: typeof HangSchema.Type) =>
  Layer.mergeAll(
    Layer.succeed(EventSink, {
      send: () => (hang === "salt" ? Effect.void : Effect.never),
    }),
    Layer.succeed(VisitorSalt, {
      salt: hang === "salt" ? Effect.never : Effect.succeed("salt"),
    }),
  );

const malformedTarget = Arbitrary.schema(Schema.Literals(["//", "///", "//a b/", "https://"]));

describe("capture first", () => {
  it.live.prop(
    "a hung salt or sink never holds the response past the capture timeout",
    { generated: exchange, hang: Arbitrary.schema(HangSchema) },
    ({ generated, hang }) =>
      Effect.gen(function* hungCapture() {
        const bare = yield* snapshot(responseFor(generated));
        const started = yield* Clock.currentTimeMillis;

        const response = yield* withEventCapture({
          identityMode: generated.scenario.mode,
          runInBackground:
            hang === "sinkDeferred"
              ? (effect) => Effect.forkDetach(effect).pipe(Effect.asVoid)
              : (effect) => effect,
        })(Effect.succeed(responseFor(generated))).pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, requestFor(generated)),
          Effect.provide(hungLayer(hang)),
          Effect.timeout("2 seconds"),
        );

        const elapsed = (yield* Clock.currentTimeMillis) - started;
        const seen = yield* snapshot(response);

        expect(elapsed).toBeLessThan(1000);
        expect(seen.body).toStrictEqual(bare.body);
        expect(seen.headers).toStrictEqual(bare.headers);
        expect(seen.status).toBe(bare.status);
      }),
    { arbitrary: { runs: 12 } },
  );

  it.effect.prop(
    "a malformed request target or a throwing skip never defects into the caller",
    { generated: exchange, target: malformedTarget, throws: Arbitrary.schema(Schema.Boolean) },
    ({ generated, target, throws }) =>
      Effect.gen(function* malformedRequest() {
        const bare = yield* snapshot(responseFor(generated));

        const { exit } = yield* withBackground((runInBackground) =>
          withEventCapture({
            identityMode: generated.scenario.mode,
            runInBackground,
            skip: (path) => {
              if (throws) {
                throw new Error(`skip threw on ${path}`);
              }

              return false;
            },
          })(Effect.succeed(responseFor(generated))).pipe(
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              requestFor(generated).modify({ url: target }),
            ),
          ),
        ).pipe(Effect.provide(memoryEventsLayer("salt")));

        expect(Exit.isSuccess(exit)).toBe(true);

        if (Exit.isSuccess(exit)) {
          expect((yield* snapshot(exit.value)).body).toStrictEqual(bare.body);
        }
      }),
  );

  it.effect.prop(
    "a broken sink or salt never changes the status, headers or body bytes, and the caller still succeeds",
    { generated: exchange },
    ({ generated }) =>
      Effect.gen(function* brokenSinkModel() {
        const bare = yield* snapshot(responseFor(generated));

        const healthy = yield* snapshot(
          yield* (yield* captured(generated).pipe(Effect.provide(memoryEventsLayer("salt")))).exit,
        );

        const { exit, exits } = yield* captured(generated).pipe(
          Effect.provide(brokenSink(generated.failure)),
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(exits.filter(Exit.isFailure)).toStrictEqual([]);

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
        const { exit } = yield* withBackground((runInBackground) =>
          withEventCapture({ identityMode: generated.scenario.mode, runInBackground })(
            Effect.fail({ reason }),
          ).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, requestFor(generated))),
        ).pipe(Effect.provide(brokenSink(failure)));

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
