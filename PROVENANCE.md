# Provenance

This package was extracted from the events package of [`joelhooks/rat-stack`](https://github.com/joelhooks/rat-stack) (MIT), at commit `9ecc954af2d73527b1d6e120763cea7feb57a6df`. The last change to its event sources there was `ff950ff803cabb4729c36e47b4b6bfe0684d4cb7`. Both projects share the same author and license.

## Lifted

| File                      | Change from the source                                                                                                     |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `src/basin.ts`            | Unchanged. Resource ids, logical names, retention and the `events_raw` table are identical.                                |
| `src/capture.ts`          | A hard-coded skipped path became the `skip` option. Added `captureQuery`, which defaults to off, and a fixed `host` label. |
| `src/event-sink.ts`       | Service key renamed to `alchemy-analytics/EventSink`.                                                                      |
| `src/event-sink-error.ts` | Unchanged.                                                                                                                 |
| `src/memory.ts`           | Service key renamed.                                                                                                       |
| `src/request-facts.ts`    | Unchanged.                                                                                                                 |
| `src/request-geo.ts`      | Imports `RequestGeoSchema` and `isGeoLabel` locally instead of from a sibling package.                                     |
| `src/schemas.ts`          | Inlined `RequestGeoSchema`. `EventSourceSchema` gained `"node"`. Added `rawEventSchema`, `toRawEvent` and `fromRawEvent`.  |
| `src/visitor.ts`          | Unchanged. The visitor cookie keeps its `rat_vid` name so existing persistent ids survive a switch.                        |
| `src/visitor-salt.ts`     | Service key renamed.                                                                                                       |
| `src/web-crypto.ts`       | Unchanged.                                                                                                                 |
| `test/*`                  | `arbitraries.ts` and the capture, request-facts, request-geo and visitor model tests, with generic hosts.                  |

Crash archiving from the source package stays there. It is specific to that application.

## New

`src/capture-event.ts`, `src/ingest.ts` and `src/node.ts`, with the capture-first, privacy, schema-extension, ingest and Node-sink tests.

## Saved queries

`docs/queries.md` generalizes four Basin SQL examples from a downstream consumer's notes. Tenant fields, route ids and hostnames were replaced with the base event fields.
