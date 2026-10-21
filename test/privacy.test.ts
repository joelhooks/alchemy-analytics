import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { RawEventSchema, RequestBodySchema, withEventCapture } from "../src/index.js";
import { EventSinkMemory, memoryEventsLayer } from "../src/memory.js";
import { MAX_HEADER_LENGTH, MAX_QUERY_VALUE_LENGTH } from "../src/request-facts.js";
import { VISITOR_COOKIE } from "../src/visitor.js";
import { idSegment, secret, slug, word } from "./arbitraries.js";
import { withBackground } from "./support.js";

const Shape = Schema.Struct({
  bodyKind: Schema.Literals(["json", "form", "text"]),
  html: Schema.Boolean,
  listQuery: Schema.Boolean,
  mode: Schema.Literals(["daily", "persistent"]),
  visitorCookie: Schema.Boolean,
});

const pathPart = Arbitrary.flatMap(Arbitrary.schema(Schema.Boolean), (isId) =>
  isId ? idSegment : slug.pipe(Arbitrary.map((value) => ({ marker: value, value }))),
);

const leakyRequest = Arbitrary.all({
  allowedValues: Arbitrary.array(word, { maxLength: 4 }),
  apiKey: secret,
  authorization: secret,
  bodySecret: secret,
  cookieSecret: secret,
  deniedSecrets: Arbitrary.array(secret, { maxLength: 4, minLength: 1 }),
  email: secret.pipe(Arbitrary.map((local) => `${local}@mail.example.test`)),
  headerTail: secret,
  longTail: secret,
  password: secret,
  path: Arbitrary.array(pathPart, { maxLength: 4 }),
  pathId: idSegment,
  referrerPath: Arbitrary.array(pathPart, { maxLength: 3, minLength: 1 }),
  referrerSecret: secret,
  responseSecret: secret,
  shape: Arbitrary.schema(Shape),
  userAgentSecret: secret,
});

type Leaky = typeof leakyRequest extends Arbitrary.Arbitrary<infer A> ? A : never;

type Part = Leaky["path"][number];

const urlPath = (parts: readonly Part[]) =>
  `/${parts.map(({ value }) => encodeURIComponent(value)).join("/")}`;

const markedPath = (parts: readonly Part[]) => `/${parts.map(({ marker }) => marker).join("/")}`;

const pathOf = (leaky: Leaky) => [...leaky.path, leaky.pathId];

const bodyOf = (leaky: Leaky) => {
  const fields = {
    email: leaky.email,
    note: leaky.bodySecret,
    password: leaky.password,
  };

  const bodies = {
    form: {
      body: new URLSearchParams(fields).toString(),
      type: "application/x-www-form-urlencoded",
    },
    json: { body: JSON.stringify(fields), type: "application/json" },
    text: {
      body: `${leaky.email} ${leaky.bodySecret} ${leaky.password}`,
      type: "text/plain",
    },
  };

  return bodies[leaky.shape.bodyKind];
};

const longValue = (leaky: Leaky) => `${"x".repeat(MAX_QUERY_VALUE_LENGTH)}${leaky.longTail}`;

const allowList = (leaky: Leaky) =>
  leaky.shape.listQuery ? [...leaky.allowedValues.map((_, index) => `a${index}`), "q"] : [];

const userAgentOf = (leaky: Leaky) =>
  `Mozilla/5.0 (${"x".repeat(MAX_HEADER_LENGTH)}) ${leaky.userAgentSecret}`;

const padding = "x".repeat(MAX_HEADER_LENGTH);

const requestOf = (leaky: Leaky) => {
  const { body, type } = bodyOf(leaky);
  const query = new URLSearchParams([
    ...leaky.allowedValues.map((value, index): [string, string] => [`a${index}`, value]),
    ...leaky.deniedSecrets.map((value, index): [string, string] => [`d${index}`, value]),
    ["q", longValue(leaky)],
    ["email", leaky.email],
    ["token", leaky.password],
  ]);

  const cookies = [
    `session=${leaky.cookieSecret}`,
    `auth=${leaky.authorization}`,
    ...(leaky.shape.visitorCookie
      ? [`${VISITOR_COOKIE}=0199a3b2-6f1e-7c3d-8a4b-1c2d3e4f5a6b`]
      : []),
  ];

  return HttpServerRequest.fromWeb(
    new Request(`https://app.example.test${urlPath(pathOf(leaky))}?${query.toString()}`, {
      body,
      headers: {
        accept: `${leaky.shape.html ? "text/html" : "application/json"};${padding}${leaky.headerTail}`,
        authorization: `Bearer ${leaky.authorization}`,
        "cf-connecting-ip": "198.51.100.23",
        "content-type": type,
        cookie: cookies.join("; "),
        host: `app.example.test${padding}${leaky.headerTail}`,
        "proxy-authorization": `Basic ${leaky.authorization}`,
        referer: `https://ref.example.test${urlPath(leaky.referrerPath)}?code=${leaky.referrerSecret}#${leaky.referrerSecret}`,
        "user-agent": userAgentOf(leaky),
        "x-api-key": leaky.apiKey,
      },
      method: "POST",
    }),
  );
};

const encodeEvent = Schema.encodeEffect(RawEventSchema);

const decodeBody = Schema.decodeUnknownEffect(RequestBodySchema);

/** Path ids at least this long are unique enough to search the whole event for. */
const SEARCHABLE_LENGTH = 16;

describe("privacy", () => {
  it.effect.prop(
    "bodies, emails, cookies, credentials, path ids, long headers and unlisted query values never reach an event",
    { leaky: leakyRequest },
    ({ leaky }) =>
      Effect.gen(function* neverLeak() {
        yield* withBackground((runInBackground) =>
          withEventCapture({
            captureQuery: allowList(leaky),
            identityMode: leaky.shape.mode,
            runInBackground,
          })(
            Effect.succeed(
              HttpServerResponse.text(`private ${leaky.responseSecret}`, {
                contentType: `${leaky.shape.html ? "text/html" : "application/json"};${padding}${leaky.responseSecret}`,
                headers: { "x-response-token": leaky.responseSecret },
              }),
            ),
          ).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, requestOf(leaky))),
        );

        const events = yield* (yield* EventSinkMemory).events;

        expect(events).toHaveLength(1);

        const [event] = events;
        const body = yield* decodeBody(event?.body);

        const encoded = JSON.stringify(
          yield* Effect.all(events.map((captured) => encodeEvent(captured))),
        );

        const neverCaptured = [
          leaky.apiKey,
          leaky.authorization,
          leaky.bodySecret,
          leaky.cookieSecret,
          leaky.email,
          leaky.headerTail,
          leaky.longTail,
          leaky.password,
          leaky.referrerSecret,
          leaky.responseSecret,
          leaky.userAgentSecret,
          ...leaky.deniedSecrets,
          ...[...pathOf(leaky), ...leaky.referrerPath]
            .filter(({ marker, value }) => marker !== value && value.length >= SEARCHABLE_LENGTH)
            .map(({ value }) => value),
          "198.51.100.23",
        ];

        for (const value of neverCaptured) {
          expect(encoded).not.toContain(value);
        }

        expect(body.path).toBe(markedPath(pathOf(leaky)));
        expect(body.referrer).toBe(`https://ref.example.test${markedPath(leaky.referrerPath)}`);
        expect(event?.server.userAgent).toBe(userAgentOf(leaky).slice(0, MAX_HEADER_LENGTH));
        expect(event?.server.host.length).toBe(MAX_HEADER_LENGTH);
        expect(body.accept?.length).toBe(MAX_HEADER_LENGTH);
        expect(body.contentType?.length).toBe(MAX_HEADER_LENGTH);

        expect(body.query).toStrictEqual(
          leaky.shape.listQuery
            ? {
                ...Object.fromEntries(
                  leaky.allowedValues.map((value, index) => [`a${index}`, [value]]),
                ),
                q: ["x".repeat(MAX_QUERY_VALUE_LENGTH)],
              }
            : {},
        );
      }).pipe(Effect.provide(memoryEventsLayer("privacy-salt"))),
    { arbitrary: { runs: 300 } },
  );
});
