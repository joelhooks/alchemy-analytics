import { readFileSync } from "node:fs";

import { describe, expect, it } from "@effect/vitest";

const manifest: unknown = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf-8"),
);

describe("package metadata", () => {
  it("names the public repository, the license and that importing has no side effects", () => {
    expect(manifest).toMatchObject({
      license: "MIT",
      repository: { url: "git+https://github.com/joelhooks/alchemy-analytics.git" },
      sideEffects: false,
    });
  });
});
