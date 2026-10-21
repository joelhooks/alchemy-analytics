import { Effect, Fiber, Layer, Redacted, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";

import { EventSink, EventSinkError, RawEventSchema, VisitorSalt } from "../src/index.js";
import type { RawEvent } from "../src/index.js";

export const SECRET = Redacted.make("ingest-secret-for-tests");

export interface PullTrackedBody {
  readonly pulledBytes: () => number;
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly wasPulled: () => boolean;
}

export const trackedRequest = (
  url: string,
  headers: Readonly<Record<string, string>>,
  chunks: readonly Uint8Array[],
  method: "GET" | "POST" = "POST",
): PullTrackedBody => {
  let pulls = 0;
  let bytes = 0;
  let index = 0;

  const body = new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        pulls += 1;
        const chunk = chunks[index];
        index += 1;

        if (chunk === undefined) {
          controller.close();
          return;
        }

        bytes += chunk.length;
        controller.enqueue(chunk);
      },
    },
    { highWaterMark: 0 },
  );

  const web =
    method === "GET"
      ? new Request(url, { headers, method })
      : new Request(url, { body, duplex: "half", headers, method: "POST" });

  return {
    pulledBytes: () => bytes,
    request: HttpServerRequest.fromWeb(web),
    wasPulled: () => pulls > 0,
  };
};

export const textChunks = (text: string, size: number) => {
  const bytes = new TextEncoder().encode(text);
  const chunks: Uint8Array[] = [];

  for (let offset = 0; offset < bytes.length; offset += size) {
    chunks.push(bytes.slice(offset, offset + size));
  }

  return chunks;
};

const encodeEvent = Schema.encodeSync(RawEventSchema);

export const batchJson = (events: readonly RawEvent[]) =>
  JSON.stringify({ events: events.map((event) => encodeEvent(event)) });

export const rawEvent = Arbitrary.schema(RawEventSchema);

export const SinkFailureSchema = Schema.Literals(["fail", "die", "dieSalt"]);

export type SinkFailure = typeof SinkFailureSchema.Type;

export const brokenSink = (failure: SinkFailure) =>
  Layer.mergeAll(
    Layer.succeed(EventSink, {
      send: () =>
        failure === "fail"
          ? Effect.fail(new EventSinkError({ cause: new Error("stream down") }))
          : Effect.die(new Error("sink threw")),
    }),
    Layer.succeed(VisitorSalt, {
      salt:
        failure === "dieSalt"
          ? Effect.die(new Error("salt threw"))
          : Effect.succeed("property-salt"),
    }),
  );

/**
 * A `runInBackground` that behaves like `waitUntil`: it forks the effect off the
 * response path and returns at once. `settle` waits for every forked effect and
 * returns their exits, so a test can see a background failure the response never would.
 */
export const makeBackground = Effect.sync(() => {
  const fibers: Fiber.Fiber<void>[] = [];

  return {
    run: (effect: Effect.Effect<void>) =>
      Effect.forkDetach(effect).pipe(
        Effect.flatMap((fiber) => Effect.sync(() => fibers.push(fiber))),
        Effect.asVoid,
      ),
    settle: Effect.suspend(() => Fiber.awaitAll(fibers.splice(0))),
  };
});

/** Runs a captured handler with a deferred background and waits for the background. */
export const withBackground = <A, E, R>(
  handler: (
    runInBackground: (effect: Effect.Effect<void>) => Effect.Effect<void>,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* runWithBackground() {
    const background = yield* makeBackground;
    const exit = yield* Effect.exit(handler(background.run));
    const exits = yield* background.settle;

    return { exit, exits };
  });
