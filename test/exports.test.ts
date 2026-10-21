import { describe, expect, it } from "@effect/vitest";

import * as root from "../src/index.js";

/** Every root export is a semver commitment; internals live behind their own modules. */
const PUBLIC_SURFACE = [
  "AnonymousIdSchema",
  "DEFAULT_CAPTURE_TIMEOUT",
  "EventSink",
  "EventSinkError",
  "EventSourceSchema",
  "IdentityModeSchema",
  "MessageIdSchema",
  "RawEventSchema",
  "RequestBodySchema",
  "RequestGeoSchema",
  "ServerContextSchema",
  "VISITOR_COOKIE",
  "VisitorSalt",
  "captureEvent",
  "fromRawEvent",
  "rawEventSchema",
  "redactIdSegments",
  "toRawEvent",
  "withEventCapture",
];

describe("package surface", () => {
  it("the root entry exports exactly the public surface", () => {
    expect(Object.keys(root).toSorted()).toStrictEqual(PUBLIC_SURFACE.toSorted());
  });
});
