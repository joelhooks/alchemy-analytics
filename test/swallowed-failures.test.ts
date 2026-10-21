import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Logger, Redacted, Schema } from "effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { TestClock } from "effect/testing";

import { EventSink, captureEvent, withEventCapture } from "../src/index.js";
import { nodeEventSink } from "../src/node.js";
import { SinkFailureSchema, brokenSink, rawEvent } from "./support.js";
import * as Arbitrary from "effect/Arbitrary";

interface Line {
  readonly level: string;
  readonly text: string;
}

const textOf = (part: unknown): string => {
  if (Cause.isCause(part)) {
    return Cause.pretty(part);
  }

  return typeof part === "string" ? part : String(part);
};

/** Every log line, warnings and below, with any cause rendered into the text. */
const recordLogs = (lines: Line[]) =>
  Logger.layer([
    Logger.make((log) => {
      const parts = Array.isArray(log.message) ? log.message : [log.message];

      lines.push({
        level: log.logLevel,
        text: [...parts.map(textOf), Cause.pretty(log.cause)].join(" "),
      });
    }),
  ]);

const warnings = (lines: readonly Line[]) => lines.filter((line) => line.level === "Warn");

const Body = Schema.Struct({ type: Schema.Literal("ping") });

const request = HttpServerRequest.fromWeb(new Request("https://app.example.test/page"));

describe("swallowed failures stay visible", () => {
  it.effect.prop(
    "captureEvent logs exactly one warning with the cause when the sink fails or dies",
    { failure: Arbitrary.schema(Schema.Literals(["fail", "die"])) },
    ({ failure }) =>
      Effect.gen(function* sinkFailure() {
        const lines: Line[] = [];

        yield* captureEvent({ anonymousId: "installation-id", body: Body, host: "cli" })({
          type: "ping",
        }).pipe(Effect.provide(Layer.mergeAll(brokenSink(failure), recordLogs(lines))));

        expect(warnings(lines)).toHaveLength(1);
        expect(warnings(lines)[0]?.text).toContain(
          failure === "fail" ? "stream down" : "sink threw",
        );
      }),
    { arbitrary: { runs: 20 } },
  );

  it.effect.prop(
    "withEventCapture logs exactly one warning with the cause for any sink or salt failure",
    { failure: Arbitrary.schema(SinkFailureSchema) },
    ({ failure }) =>
      Effect.gen(function* captureFailure() {
        const lines: Line[] = [];

        const exit = yield* withEventCapture({
          identityMode: "persistent",
          runInBackground: (effect) => effect,
        })(Effect.succeed(HttpServerResponse.text("ok"))).pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, request),
          Effect.provide(Layer.mergeAll(brokenSink(failure), recordLogs(lines))),
          Effect.exit,
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(warnings(lines)).toHaveLength(1);

        const expected = { die: "sink threw", dieSalt: "salt threw", fail: "stream down" }[failure];

        expect(warnings(lines)[0]?.text).toContain(expected);
      }),
    { arbitrary: { runs: 20 } },
  );

  it.effect.prop(
    "the Node sink logs exactly one warning with the cause when the transport defects",
    { event: rawEvent },
    ({ event }) =>
      Effect.gen(function* transportDefect() {
        const lines: Line[] = [];

        const client = Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((clientRequest) =>
            Effect.fail(
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({
                  description: "transport down",
                  request: clientRequest,
                }),
              }),
            ),
          ),
        );

        yield* Effect.gen(function* send() {
          const sink = yield* EventSink;
          yield* sink.send([event]);
          yield* TestClock.adjust("10 seconds");
        }).pipe(
          Effect.provide(
            nodeEventSink({
              secret: Redacted.make("secret"),
              url: "http://127.0.0.1:1/ingest",
            }).pipe(Layer.provide(client), Layer.provideMerge(recordLogs(lines))),
          ),
          Effect.scoped,
        );

        expect(warnings(lines)).toHaveLength(1);
        expect(warnings(lines)[0]?.text).toContain("transport down");
      }),
    { arbitrary: { runs: 10 } },
  );
});
