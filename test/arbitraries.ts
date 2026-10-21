import { Schema } from "effect";
import * as Arbitrary from "effect/Arbitrary";

const wordCharacters = Schema.Literals([
  "a",
  "e",
  "k",
  "o",
  "s",
  "t",
  "u",
  "x",
  "0",
  "7",
  "-",
  "_",
]);

export const word = Arbitrary.array(Arbitrary.schema(wordCharacters), {
  maxLength: 10,
  minLength: 1,
}).pipe(Arbitrary.map((characters) => characters.join("")));

const slugCharacters = Schema.Literals(["a", "e", "k", "o", "s", "t", "u", "x", "-"]);

/** Path-segment words with no digits, so the id redactor never touches them. */
export const slug = Arbitrary.array(Arbitrary.schema(slugCharacters), {
  maxLength: 12,
  minLength: 1,
}).pipe(Arbitrary.map((characters) => characters.join("")));

const hexDigit = Arbitrary.schema(
  Schema.Literals(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]),
);

/** A unique 18-character marker that no event field can contain by accident. */
export const secret = Arbitrary.array(hexDigit, { maxLength: 16, minLength: 16 }).pipe(
  Arbitrary.map((digits) => `zq${digits.join("")}`),
);

/** Values that look like ids in a path: emails, UUIDs, digit runs and opaque tokens. */
const IdKindSchema = Schema.Literals(["email", "uuid", "digits", "token", "hex"]);

const idValue = {
  digits: Arbitrary.schema(
    Schema.Int.check(Schema.isBetween({ maximum: 99_999_999, minimum: 1000 })),
  ).pipe(Arbitrary.map(String)),
  email: secret.pipe(Arbitrary.map((local) => `${local}@mail.example.test`)),
  hex: Arbitrary.array(hexDigit, { maxLength: 40, minLength: 16 }).pipe(
    Arbitrary.map((digits) => digits.join("")),
  ),
  token: secret.pipe(Arbitrary.map((value) => `${value}9`)),
  uuid: Arbitrary.schema(Schema.String.check(Schema.isUUID())),
};

/** Values that look like ids in a path: emails, UUIDs, digit runs and opaque tokens. */
export const idSegment = Arbitrary.flatMap(Arbitrary.schema(IdKindSchema), (kind) =>
  idValue[kind].pipe(
    Arbitrary.map((value) => ({ marker: kind === "email" ? ":email" : ":id", value })),
  ),
);

export const sensitiveWord = Arbitrary.schema(
  Schema.Literals([
    "token",
    "access_token",
    "Email",
    "api_key",
    "password",
    "session_id",
    "code",
    "sig",
    "SECRET",
  ]),
);

export const queryKey = Arbitrary.flatMap(Arbitrary.schema(Schema.Boolean), (sensitive) =>
  sensitive ? sensitiveWord : word,
);

export const queryPairs = Arbitrary.array(Arbitrary.all([queryKey, word]), {
  maxLength: 12,
});
