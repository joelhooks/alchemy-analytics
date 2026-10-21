// Runs inside a temp project that installed the packed package. Sends one event through
// the Node sink to a local ingest route and exits non-zero unless exactly one arrives.
import { createServer } from "node:http";

import { Effect, Layer, Redacted, Schema } from "effect";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { captureEvent } from "alchemy-analytics";
import { ingestEvents } from "alchemy-analytics/ingest";
import { EventSinkMemory, memoryEventsLayer } from "alchemy-analytics/memory";
import { NodeEventSink } from "alchemy-analytics/node";

const secret = Redacted.make("consumer-secret");

const memory = await Effect.runPromise(Effect.scoped(Layer.build(memoryEventsLayer("salt"))));

const server = createServer((incoming, outgoing) => {
  const chunks = [];
  incoming.on("data", (chunk) => chunks.push(chunk));
  incoming.on("end", () => {
    const request = new Request(`http://127.0.0.1${incoming.url}`, {
      body: Buffer.concat(chunks),
      headers: incoming.headers,
      method: incoming.method,
    });

    void Effect.runPromise(
      ingestEvents({ secret }).pipe(
        Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(request)),
        Effect.provideContext(memory),
      ),
    ).then(async (response) => {
      const web = HttpServerResponse.toWeb(response);
      outgoing.writeHead(web.status, Object.fromEntries(web.headers));
      outgoing.end(Buffer.from(await web.arrayBuffer()));
    });
  });
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const { port } = server.address();

const Body = Schema.Struct({ command: Schema.String, type: Schema.Literal("command") });

const track = captureEvent({ anonymousId: "installation-id", body: Body, host: "cli" });

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* send() {
      yield* Layer.build(
        NodeEventSink({ secret, url: `http://127.0.0.1:${port}/_analytics/ingest` }),
      ).pipe(
        Effect.flatMap((context) =>
          track({ command: "sync", type: "command" }).pipe(Effect.provideContext(context)),
        ),
      );
    }),
  ),
);

const received = await Effect.runPromise(
  Effect.gen(function* read() {
    return yield* (yield* EventSinkMemory).events;
  }).pipe(Effect.provideContext(memory)),
);

server.close();
console.log(`events received: ${received.length}`);
process.exit(received.length === 1 ? 0 : 1);
