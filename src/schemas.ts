import { Effect, Schema } from "effect";

export const GEO_LABEL_MAX_LENGTH = 100;

const GeoLabelSchema = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(GEO_LABEL_MAX_LENGTH),
);

export const isGeoLabel = Schema.is(GeoLabelSchema);

export const RequestGeoSchema = Schema.Struct({
  asOrganization: Schema.optionalKey(GeoLabelSchema),
  asn: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  city: Schema.optionalKey(GeoLabelSchema),
  colo: Schema.optionalKey(GeoLabelSchema),
  continent: Schema.optionalKey(GeoLabelSchema),
  country: Schema.optionalKey(GeoLabelSchema),
  isEUCountry: Schema.optionalKey(Schema.Boolean),
  region: Schema.optionalKey(GeoLabelSchema),
  regionCode: Schema.optionalKey(GeoLabelSchema),
  timezone: Schema.optionalKey(GeoLabelSchema),
});

export type RequestGeo = typeof RequestGeoSchema.Type;

export const AnonymousIdSchema = Schema.NonEmptyString.pipe(Schema.brand("AnonymousId"));

export type AnonymousId = typeof AnonymousIdSchema.Type;

export const MessageIdSchema = Schema.String.check(Schema.isUUID(7)).pipe(
  Schema.brand("MessageId"),
);

export type MessageId = typeof MessageIdSchema.Type;

export const IdentityModeSchema = Schema.Literals(["daily", "persistent"]);

export type IdentityMode = typeof IdentityModeSchema.Type;

export const EventSourceSchema = Schema.Literals(["worker", "node"]);

export type EventSource = typeof EventSourceSchema.Type;

export const ServerContextSchema = Schema.Struct({
  ...RequestGeoSchema.fields,
  host: Schema.String,
  ipHash: Schema.optionalKey(Schema.String),
  receivedAt: Schema.String,
  userAgent: Schema.optionalKey(Schema.String),
});

export type ServerContext = typeof ServerContextSchema.Type;

export const RequestBodySchema = Schema.Struct({
  accept: Schema.optionalKey(Schema.String),
  contentType: Schema.optionalKey(Schema.String),
  durationMs: Schema.Finite,
  method: Schema.String,
  path: Schema.String,
  query: Schema.Record(Schema.String, Schema.Array(Schema.String)),
  referrer: Schema.optionalKey(Schema.String),
  signedAgent: Schema.Boolean,
  status: Schema.Int,
  type: Schema.Literal("request"),
});

export type RequestBody = typeof RequestBodySchema.Type;

const rawEventFields = {
  anonymousId: AnonymousIdSchema,
  identityMode: IdentityModeSchema,
  messageId: MessageIdSchema,
  server: ServerContextSchema,
  source: EventSourceSchema,
  v: Schema.Literal(1),
};

export const RawEventSchema = Schema.Struct({
  ...rawEventFields,
  body: Schema.Json,
});

export type RawEvent = typeof RawEventSchema.Type;

export type BodySchema = Schema.Codec<unknown, Schema.Json>;

export const rawEventSchema = <Body extends BodySchema>(body: Body) =>
  Schema.Struct({ ...rawEventFields, body });

export type RawEventOf<Body extends BodySchema> = ReturnType<typeof rawEventSchema<Body>>["Type"];

const decodeRawEvent = Schema.decodeUnknownEffect(RawEventSchema);

export const toRawEvent =
  <Body extends BodySchema>(schema: ReturnType<typeof rawEventSchema<Body>>) =>
  (event: RawEventOf<Body>) =>
    Schema.encodeEffect(schema)(event).pipe(Effect.flatMap(decodeRawEvent));

export const fromRawEvent = <Body extends BodySchema>(
  schema: ReturnType<typeof rawEventSchema<Body>>,
) => {
  const encodeRaw = Schema.encodeEffect(RawEventSchema);
  const decode = Schema.decodeUnknownEffect(schema);

  return (event: RawEvent) => encodeRaw(event).pipe(Effect.flatMap(decode));
};
