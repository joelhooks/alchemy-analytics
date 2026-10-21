import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    maxWorkers: Number(process.env.VITEST_MAX_WORKERS ?? "4"),
    reporters: ["verbose"],
    testTimeout: 30_000,
  },
});
