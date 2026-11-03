// Writes test/fixtures/visitor-digests-4.0.json from a built checkout of 9d9c3e1 (Effect 4.0.0).
// Usage: pnpm build && SOURCE_COMMIT=$(git rev-parse --short HEAD) node scripts/capture-visitor-digests.mjs <out>
import { readFileSync, writeFileSync } from "node:fs";

const [out] = process.argv.slice(2);
const root = new URL("../", import.meta.url);
const { Effect } = await import(new URL("node_modules/effect/dist/index.js", root).href);
const { identitySalt, resolveVisitor, saltedHash } = await import(new URL("dist/visitor.js", root).href);
const { webCryptoLayer } = await import(new URL("dist/web-crypto.js", root).href);
const effectVersion = JSON.parse(readFileSync(new URL("node_modules/effect/package.json", root), "utf-8")).version;

let seed = 0x9e3779b9;
const next = () => {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
  return seed;
};
const pick = (items) => items[next() % items.length];
const alphabet = ["a", "Z", "0", "-", "_", " ", ":", "|", "\u0000", "é", "ß", "日", "本", "😀", "\u200b", "/", "=", "%"];
const text = (max) => Array.from({ length: next() % (max + 1) }, () => pick(alphabet)).join("");
const day = () =>
  `20${26 + (next() % 4)}-${String(1 + (next() % 12)).padStart(2, "0")}-${String(1 + (next() % 28)).padStart(2, "0")}`;
const ips = ["203.0.113.1", "203.0.113.254", "198.51.100.9", "192.0.2.77", "2001:db8::1", "2001:db8:85a3::8a2e:370:7334"];
const agents = ["Mozilla/5.0 test", "curl/8", "Example-Agent/1.0 (example.test)", ""];

const run = (effect) => Effect.runPromise(effect.pipe(Effect.provide(webCryptoLayer)));

const salted = [];
for (let index = 0; index < 48; index += 1) {
  const salt = text(24);
  const parts = Array.from({ length: next() % 5 }, () => text(16));
  salted.push({ salt, parts, hex: await run(saltedHash(salt, parts)) });
}

const dailySalts = [];
for (let index = 0; index < 16; index += 1) {
  const salt = text(24);
  const on = day();
  dailySalts.push({ salt, day: on, hex: await run(identitySalt(salt, "daily", on)) });
}

const dailyVisitors = [];
for (let index = 0; index < 24; index += 1) {
  const recorded = {
    day: day(),
    ip: next() % 5 === 0 ? null : pick(ips),
    salt: text(24),
    userAgent: next() % 5 === 0 ? null : pick(agents),
  };
  const visitor = await run(
    resolveVisitor({
      cookie: undefined,
      day: recorded.day,
      ip: recorded.ip ?? undefined,
      mode: "daily",
      salt: recorded.salt,
      userAgent: recorded.userAgent ?? undefined,
    }),
  );
  dailyVisitors.push({ ...recorded, anonymousId: visitor.anonymousId });
}

const dailyIpHashes = [];
for (let index = 0; index < 16; index += 1) {
  const salt = text(24);
  const on = day();
  const ip = pick(ips);
  const hex = await run(
    identitySalt(salt, "daily", on).pipe(Effect.flatMap((idSalt) => saltedHash(idSalt, ["ip", ip]))),
  );
  dailyIpHashes.push({ salt, day: on, ip, hex });
}

writeFileSync(
  out,
  `${JSON.stringify({ capturedWith: { commit: process.env.SOURCE_COMMIT, effect: effectVersion }, salted, dailySalts, dailyVisitors, dailyIpHashes }, null, 2)}\n`,
);
