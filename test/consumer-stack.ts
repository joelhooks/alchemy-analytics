import { Basin } from "alchemy-analytics/basin";
import { withIngestRoute } from "alchemy-analytics/ingest";
import { NodeEventSink } from "alchemy-analytics/node";
import { captureEvent, rawEventSchema, toRawEvent, withEventCapture } from "alchemy-analytics";
import type { EventSink, RawEvent, RawEventOf, VisitorSalt } from "alchemy-analytics";
import { Effect, Redacted, Schema } from "effect";
import type { Layer } from "effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

const app = Effect.succeed(HttpServerResponse.text("ok"));

export const workerEvents: Layer.Layer<EventSink | VisitorSalt, unknown, unknown> = Basin({
  id: "App",
});

export const workerFetch = withIngestRoute({ secret: Redacted.make("secret") })(
  withEventCapture({
    identityMode: "daily",
    runInBackground: (effect) => effect,
  })(app),
);

const CliBody = Schema.Struct({
  command: Schema.String,
  exitCode: Schema.Int,
  type: Schema.Literal("command"),
});

export const CliEvent = rawEventSchema(CliBody);

export type CliCommandEvent = RawEventOf<typeof CliBody>;

export const encodeCliEvent: (
  event: CliCommandEvent,
) => Effect.Effect<RawEvent, Schema.SchemaError> = toRawEvent(CliEvent);

const track = captureEvent({
  anonymousId: "installation-id",
  body: CliBody,
  host: "cli",
});

export const program: Effect.Effect<void> = track({
  command: "sync",
  exitCode: 0,
  type: "command",
}).pipe(
  Effect.provide(
    NodeEventSink({
      secret: Redacted.make("secret"),
      url: "https://app.example.test/_analytics/ingest",
    }),
  ),
);
