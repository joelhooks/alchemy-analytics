import { readFileSync } from "node:fs";

import { describe, expect, it } from "@effect/vitest";

const manifest: unknown = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf-8"),
);

const ignored = new Set(
  readFileSync(new URL("../.gitignore", import.meta.url), "utf-8")
    .split("\n")
    .map((line) => line.trim()),
);

describe("package metadata", () => {
  it("ignores local agent notes so a broad add can never publish them", () => {
    expect([".pi/", ".brain/", "BRAIN.md"].filter((entry) => !ignored.has(entry))).toStrictEqual(
      [],
    );
  });

  it("names the public repository, the license and that importing has no side effects", () => {
    expect(manifest).toMatchObject({
      license: "MIT",
      repository: { url: "git+https://github.com/joelhooks/alchemy-analytics.git" },
      sideEffects: false,
    });
  });
});
