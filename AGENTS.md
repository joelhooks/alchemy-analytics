# Agent instructions

Capture-first, privacy-first request analytics for [Alchemy](https://alchemy.run) and [Effect](https://effect.website).

- The repo is public. It carries no hostnames, account ids, project or customer names, private paths or tokens. Examples use `example.com` and `example.test` hosts and generic ids such as `app`.
- Capture never changes a response or a caller's result. Every sink, salt and transport failure is logged and swallowed. Change a guard only together with a model test that breaks when the guard does. `test/capture-first.model.test.ts` and `test/node-sink.test.ts` are those tests.
- Privacy is an allow-list. Add a captured field only after extending `test/privacy.test.ts` so that it generates the new input with secrets in it.
- `Basin` resource ids (`${id}EventsBucket`, `${id}EventsStream`, `${id}VisitorSalt`, `${id}EventsCatalog`, `${id}EventsSink`, `${id}EventsPipeline`) and the `events_raw` table are a compatibility contract with existing deployments. Renaming one replaces a retained resource.
- The ingest route authenticates before it reads the body. Keep the order: method, then auth, then declared length, then streamed length, then batch bound, then decode.
- Effect is a `>=4.0.0 <4.1.0` peer so consumers share one runtime; the dev pin stays `4.0.0`. Effect `4.0.x` changed `Crypto.randomUUIDv7` from a value to a method, so the package builds its own UUIDv7 in `web-crypto.ts`. `pnpm test:matrix` runs the suite on 4.0.0 and 4.0.5. Alchemy stays an optional peer; only `src/basin.ts` imports it.
- The Node sink drain is interruptible. Taking from the queue moves events into a pending `Ref` in one uninterruptible step, so an interrupt loses at most the in-flight POST. Never make the whole drain uninterruptible or detach the loop: close then waits on it, or it posts after close.
- Effect `4.0.0` `Queue.takeBetween(queue, 0, n)` returns an empty array even when the queue holds items. The Node sink drains with `Queue.clear` instead.
- The root entry exports only the public surface pinned in `test/exports.test.ts`. Every root export is a semver commitment, so internal helpers stay in their own modules.
- Gates: `pnpm build && pnpm typecheck && pnpm lint && pnpm test`. Prefer property and model tests over examples.
- See `PROVENANCE.md` for where the code came from.
