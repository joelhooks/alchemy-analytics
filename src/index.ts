export { DEFAULT_CAPTURE_TIMEOUT, withEventCapture } from "./capture.js";

export type { CaptureOptions } from "./capture.js";

export { captureEvent } from "./capture-event.js";

export type { CaptureEventOptions } from "./capture-event.js";

export { EventSink } from "./event-sink.js";

export { EventSinkError } from "./event-sink-error.js";

export { VisitorSalt } from "./visitor-salt.js";

export { redactIdSegments } from "./request-facts.js";

export type { PathRedaction } from "./request-facts.js";

export {
  AnonymousIdSchema,
  EventSourceSchema,
  IdentityModeSchema,
  MessageIdSchema,
  RawEventSchema,
  RequestBodySchema,
  RequestGeoSchema,
  ServerContextSchema,
  fromRawEvent,
  rawEventSchema,
  toRawEvent,
} from "./schemas.js";

export type {
  AnonymousId,
  BodySchema,
  EventSource,
  IdentityMode,
  MessageId,
  RawEvent,
  RawEventOf,
  RequestBody,
  RequestGeo,
  ServerContext,
} from "./schemas.js";
