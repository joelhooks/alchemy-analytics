export const MAX_HEADER_LENGTH = 256;

export const MAX_PATH_LENGTH = 1024;

export const MAX_QUERY_KEYS = 20;

export const MAX_QUERY_VALUES_PER_KEY = 10;

export const MAX_QUERY_VALUE_LENGTH = 512;

export const clip = (value: string | undefined, maxLength: number) =>
  value === undefined || value.length <= maxLength ? value : value.slice(0, maxLength);

/**
 * Records query values only for keys the project names. Keys and values are bounded.
 */
export const capturedQuery = (params: URLSearchParams, allowed: readonly string[]) => {
  const allow = new Set(allowed);
  const grouped = new Map<string, string[]>();

  for (const [key, value] of params) {
    const values = grouped.get(key) ?? [];

    if (
      allow.has(key) &&
      (grouped.has(key) || grouped.size < MAX_QUERY_KEYS) &&
      values.length < MAX_QUERY_VALUES_PER_KEY
    ) {
      grouped.set(key, [...values, clip(value, MAX_QUERY_VALUE_LENGTH) ?? ""]);
    }
  }

  return Object.fromEntries(grouped);
};

const EMAIL = /^[^\s@/]+@[^\s@/]+\.[^\s@/]+$/u;

const UUID = /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/iu;

const DIGIT_RUN = /\d{4}/u;

const HEX = /^[\da-f]+$/iu;

const BASE64URL_CHARACTERS = /[\w=-]/gu;

const SEPARATORS = /[_-]/gu;

const TOKEN_MIN_LENGTH = 16;

const share = (segment: string, characters: RegExp) =>
  (segment.length - segment.replaceAll(characters, "").length) / segment.length;

const isToken = (segment: string) =>
  segment.length >= TOKEN_MIN_LENGTH &&
  (HEX.test(segment) ||
    (share(segment, BASE64URL_CHARACTERS) >= 0.9 &&
      share(segment, SEPARATORS) <= 0.1 &&
      /\d/u.test(segment) &&
      /[a-z]/iu.test(segment)));

const decoded = (segment: string) => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
};

const redactSegment = (raw: string) => {
  const segment = decoded(raw);

  if (EMAIL.test(segment)) {
    return ":email";
  }

  return UUID.test(segment) || DIGIT_RUN.test(segment) || isToken(segment) ? ":id" : raw;
};

/**
 * The default path redaction. Replaces each segment that looks like an email with
 * `:email`, and each UUID, run of 4 or more digits or high-entropy token with `:id`.
 */
export const redactIdSegments = (path: string) => path.split("/").map(redactSegment).join("/");

export type PathRedaction = false | ((path: string) => string);

export const redactorOf = (redaction: PathRedaction | undefined) =>
  redaction === false ? (path: string) => path : (redaction ?? redactIdSegments);

const originAndPath = (url: URL, redact: (path: string) => string) =>
  `${url.origin}${redact(url.pathname)}`;

export const referrerOf = (
  value: string | undefined,
  redact: (path: string) => string = redactIdSegments,
) =>
  value !== undefined && URL.canParse(value)
    ? clip(originAndPath(new URL(value), redact), MAX_PATH_LENGTH)
    : undefined;

type RequestFact =
  | boolean
  | number
  | string
  | Readonly<Record<string, readonly string[]>>
  | undefined;

export const withoutUndefined = (record: Readonly<Record<string, RequestFact>>) =>
  Object.fromEntries(Object.entries(record).filter(([, value]) => value !== undefined));
