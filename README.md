# alchemy-analytics

Capture-first, privacy-first request analytics for [Effect](https://effect.website) apps, with infrastructure declared through [Alchemy](https://alchemy.run).

A Cloudflare Worker gets one layer, `Basin({ id })`. It declares an R2 bucket, a Pipelines stream, an R2 Data Catalog, an Iceberg sink and the pipeline. Then it hands your app an `EventSink`. A Node process gets `NodeEventSink`, which posts batches to an ingest route on your own Worker.

Capture never changes a response or a caller's result. A broken sink is logged and ignored.

## Install

```sh
pnpm add alchemy-analytics effect@4.0.0
# Workers that declare infrastructure also need Alchemy:
pnpm add alchemy@2.0.0-beta.83
```

Effect is a peer dependency, held at `4.0.0`, so your app and this package share one runtime. Alchemy is an optional peer. Only `alchemy-analytics/basin` imports it.

Until a release is on npm, install from GitHub: `pnpm add github:joelhooks/alchemy-analytics`. The `prepare` script builds `dist`.

## Entry points

| Import                     | What it gives you                                                                                                           |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `alchemy-analytics`        | `withEventCapture`, `captureEvent`, `EventSink`, `VisitorSalt`, the schemas, `rawEventSchema`, `toRawEvent`, `fromRawEvent` |
| `alchemy-analytics/basin`  | `Basin`, `basinFoundation`, `basinStream`, `icebergSink`, `EVENTS_TABLE`                                                    |
| `alchemy-analytics/ingest` | `withIngestRoute`, `ingestEvents`, `IngestBatchSchema`, the ingest limits                                                   |
| `alchemy-analytics/node`   | `NodeEventSink`, `nodeEventSink`, the buffer defaults                                                                       |
| `alchemy-analytics/memory` | `memoryEventsLayer`, `EventSinkMemory` for tests                                                                            |

## Worker: one layer, one line

```ts
import { withEventCapture } from "alchemy-analytics";
import { Basin } from "alchemy-analytics/basin";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";

const inBackground = (effect: Effect.Effect<void>) =>
  Effect.gen(function* () {
    const execution = yield* Cloudflare.WorkerExecutionContext;
    yield* execution.waitUntil(effect);
  });

// Inside your Worker's Effect:
const events = yield * Layer.build(Basin({ id: "app" }));

const fetch = withEventCapture({
  identityMode: "daily",
  runInBackground: inBackground,
})(app).pipe(Effect.provideContext(events));
```

`Basin({ id })` declares these resources. Each is retained on removal.

| Logical id            | Resource                                                            |
| --------------------- | ------------------------------------------------------------------- |
| `${id}EventsBucket`   | R2 bucket                                                           |
| `${id}EventsStream`   | Pipelines stream, with public HTTP ingest off                       |
| `${id}VisitorSalt`    | Alchemy `Random`, the salt for visitor and IP hashes                |
| `${id}EventsCatalog`  | R2 Data Catalog on the bucket                                       |
| `${id}EventsSink`     | Iceberg sink: zstd Parquet, namespace `default`, table `events_raw` |
| `${id}EventsPipeline` | `INSERT INTO <sink> SELECT * FROM <stream>`                         |

The catalog, sink and pipeline are declared at deploy time only. They need `EVENTS_SINK_TOKEN`, an R2 admin read-and-write token, in the deploying process's environment. The token is never bound to the Worker. To declare only the bucket, stream and salt and send nothing, use `basinFoundation({ id })`.

### Identity modes

- `"daily"` sets no cookie. Each UTC day it derives a day salt from the stored salt, `hash(salt, "day", utcDay)`. The anonymous id is a hash of the day salt, IP and user agent, and `ipHash` is a hash of the day salt and IP. Nothing on a daily event is hashed with the permanent salt, so two days of the same visitor share no identifier.
- `"persistent"` sets a first-party `rat_vid` cookie with a random id on HTML responses. `ipHash` uses the permanent salt, so it stays stable across days.

### Capture options

| Option            | Default            | Meaning                                                                                                                                          |
| ----------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `identityMode`    | —                  | `"daily"` or `"persistent"`. See [Identity modes](#identity-modes).                                                                              |
| `runInBackground` | —                  | Runs the sink write off the response path, e.g. `waitUntil`. It should fork and return, not await the sink.                                      |
| `captureTimeout`  | `250 millis`       | The longest capture may hold the response. On timeout the response goes out unchanged and the event is dropped. Named `DEFAULT_CAPTURE_TIMEOUT`. |
| `captureCity`     | `false`            | Also records `request.cf.city`.                                                                                                                  |
| `captureQuery`    | `[]`               | Query keys whose values are recorded, e.g. `["utm_source", "utm_medium"]`. Every other key is dropped.                                           |
| `host`            | request            | A fixed service label instead of the request's `Host` header.                                                                                    |
| `redactPath`      | `redactIdSegments` | Rewrites the path and referrer path before they are recorded. `false` records them verbatim.                                                     |
| `skip`            | none               | `(path) => boolean`: paths that are never captured.                                                                                              |

## Node: a sink that posts to your own Worker

A CLI or server outside a Worker sends events to an ingest route on the project's Worker. The Worker writes them to its stream binding. Public stream ingest stays off, and no new Cloudflare token is involved. The project authenticates the route with its own secret.

On the Worker, mount the route in front of capture:

```ts
import { withIngestRoute } from "alchemy-analytics/ingest";
import { Config } from "effect";

const secret = yield * Config.Redacted("ANALYTICS_INGEST_SECRET");

const fetch = withIngestRoute({ secret })(
  withEventCapture({ identityMode: "daily", runInBackground: inBackground })(app),
).pipe(Effect.provideContext(events));
```

In the Node process:

```ts
import { captureEvent } from "alchemy-analytics";
import { NodeEventSink } from "alchemy-analytics/node";
import { Effect, Redacted, Schema } from "effect";

const track = captureEvent({
  anonymousId: installationId,
  body: Schema.Json,
  host: "cli",
});

const program = Effect.gen(function* () {
  yield* track({ command: "sync", type: "command" });
}).pipe(
  Effect.provide(
    NodeEventSink({
      secret: Redacted.make(process.env.ANALYTICS_INGEST_SECRET ?? ""),
      url: "https://app.example.com/_analytics/ingest",
    }),
  ),
);
```

`send` puts events in a bounded buffer and returns at once. A background fiber posts batches on an interval. Closing the layer's scope flushes whatever is left. A full buffer drops the new events and logs a warning. A failed or timed-out post logs a warning and drops that batch, with no retry. Closing takes at most `closeTimeout` in total, even against a hung endpoint. After close the queue is shut down, and `send` logs and drops. The caller is never blocked and never fails.

| `NodeEventSink` option | Default        | Meaning                                                                                               |
| ---------------------- | -------------- | ----------------------------------------------------------------------------------------------------- |
| `url`                  | —              | The ingest route's full URL                                                                           |
| `secret`               | —              | Sent as `Authorization: Bearer <secret>`                                                              |
| `maxBuffer`            | `1000`         | Events held before new ones are dropped                                                               |
| `maxBatch`             | `100`          | Events per POST. Keep it at or under the route's `maxEvents`                                          |
| `flushInterval`        | `"5 seconds"`  | Time between background posts                                                                         |
| `requestTimeout`       | `"10 seconds"` | Per-POST timeout                                                                                      |
| `closeTimeout`         | `"10 seconds"` | Total time scope close may spend stopping the loop and flushing. Unsent events after that are dropped |

Use `nodeEventSink(options)` to supply your own `HttpClient`. `NodeEventSink` provides the fetch client.

### Ingest route limits

The route checks in this order, and it reads the body only after authentication passes.

1. Anything except `POST` gets `405`.
2. A missing or wrong bearer secret gets `401`. The comparison runs in constant time over SHA-256 digests, and the body is never read.
3. A declared `Content-Length` over `maxBodyBytes` gets `413` before the body is read. A header that is not plain decimal digits is ignored, and the streamed count decides.
4. The body is streamed and counted. Once it passes `maxBodyBytes`, the route stops reading and returns `413`.
5. A batch with more than `maxEvents` events gets `413`, and none of it is written.
6. Any invalid event gets `400`, and none of the batch is written.
7. A sink failure gets `502`. Success is `202` with `{ "accepted": n }`.

| `withIngestRoute` option | Default                | Name in code             |
| ------------------------ | ---------------------- | ------------------------ |
| `secret`                 | —                      |                          |
| `maxBodyBytes`           | `1048576` (1 MiB)      | `DEFAULT_MAX_BODY_BYTES` |
| `maxEvents`              | `500`                  | `DEFAULT_MAX_EVENTS`     |
| `path`                   | `"/_analytics/ingest"` | `INGEST_PATH`            |

### Alternative: Pipelines HTTP ingest (not wired)

A Pipelines stream can expose its own HTTP endpoint. With Alchemy, `Cloudflare.Pipelines.Stream(name, { http: true })` enables it with API-token authentication. The flag is mutable in place, and the stream's `endpoint` output holds the URL. A client then POSTs a JSON array of events with a token that has `Pipelines Send` permission.

This package does not wire that path. It needs a Cloudflare token on every client, it opens a public endpoint, and it skips the route's own limits. Prefer the ingest route unless you have a reason not to.

## Extending the event schema

`RawEventSchema` is the wire format. Its `body` is any JSON. Give your project a typed body without forking:

```ts
import { captureEvent, rawEventSchema, toRawEvent, fromRawEvent } from "alchemy-analytics";
import type { RawEventOf } from "alchemy-analytics";
import { Schema } from "effect";

const SignupBody = Schema.Struct({
  plan: Schema.Literals(["free", "pro"]),
  seats: Schema.Int,
  type: Schema.Literal("signup"),
});

export const SignupEvent = rawEventSchema(SignupBody);
export type SignupEvent = RawEventOf<typeof SignupBody>;

const track = captureEvent({ anonymousId, body: SignupBody, host: "api" });

const encode = toRawEvent(SignupEvent); // SignupEvent -> RawEvent for any EventSink
const decode = fromRawEvent(SignupEvent); // RawEvent -> SignupEvent when reading back
```

The body schema's encoded form must be JSON. A property test round-trips a generated extended event through the base event and the ingest wire.

## Queries

[`docs/queries.md`](docs/queries.md) has Basin SQL for traffic by path, 5xx by path and release, discovery traffic, and latency percentiles.

## Privacy guarantees

Request capture records the method, path, status, duration, `Accept`, response content type, whether a `signature-agent` header was present, the referrer's origin and path, the user agent, coarse `request.cf` geography and a salted IP hash.

Paths are captured with id-like segments redacted by default. `redactIdSegments` replaces each segment that looks like an email with `:email`. It replaces each UUID, each segment with a run of 4 or more digits, and each high-entropy token with `:id`. A token here means 16 or more characters that are all hex, or mostly base64url with both letters and digits and few separators. The same redaction applies to the referrer path. So `/reset/3f9c0a6e1b2d4c5e8f7a` becomes `/reset/:id`, and `/users/ada@example.com/orders/40213` becomes `/users/:email/orders/:id`.

Readable slugs such as `/blog/how-to-use-effect` pass through, but redaction is a heuristic. If your routes carry other secrets, pass your own rule. You can also compose with the default:

```ts
import { redactIdSegments, withEventCapture } from "alchemy-analytics";

withEventCapture({
  identityMode: "daily",
  redactPath: (path) => redactIdSegments(path).replace(/^\/invite\/[^/]+/u, "/invite/:code"),
  runInBackground: inBackground,
});
```

`redactPath: false` turns redaction off and records paths verbatim.

Long values are cut, not dropped:

| Field                                               | Cap                        |
| --------------------------------------------------- | -------------------------- |
| user agent, `Accept`, response content type, `host` | 256 characters             |
| path and referrer                                   | 1024 characters            |
| each recorded query value                           | 512 characters             |
| recorded query keys                                 | 20 keys, 10 values per key |

It never records:

- request or response bodies
- query-string values, except under keys you list in `captureQuery`
- cookies, other than the package's own `rat_vid` visitor id in `persistent` mode
- `Authorization`, `Proxy-Authorization`, API-key headers or other credentials
- the raw IP address, which is only ever a salted hash
- latitude, longitude, postal code or metro code, and city only with `captureCity`
- the referrer's query string or fragment

A property test sends generated requests that carry secrets in every one of those places. It also puts them in path and referrer-path segments, past the length caps of the user agent, `Accept`, content type, `host` and query values, and under query keys that are not listed. It asserts that none of them reach an encoded event, and that listed query values and ordinary path segments still do.

## Capture-first guarantees

- `withEventCapture` returns the app's response with byte-identical status, headers and body, whatever the sink or salt does. The only thing it adds is the visitor cookie in `persistent` mode.
- Capture holds the response for at most `captureTimeout`, even when the salt or sink never answers.
- A malformed request target or a throwing `skip` is logged, and the response goes out unchanged.
- A sink failure or defect inside `runInBackground` is caught and logged there, so it never fails the background task.
- An app failure passes through unchanged.
- `captureEvent` always succeeds for its caller.
- `NodeEventSink.send` never fails and never waits on the network.

Model tests drive these guarantees with failing, throwing and hanging sinks, throwing and hanging salts, malformed request targets, and a refused, erroring or hanging ingest route. The tests run `runInBackground` as a real fork, the way `waitUntil` does. Each guard was also broken on purpose to confirm a test catches it.

## Development

```sh
pnpm install
pnpm build && pnpm typecheck && pnpm lint && pnpm test
```

## License

MIT. See [`LICENSE`](LICENSE) and [`PROVENANCE.md`](PROVENANCE.md).
