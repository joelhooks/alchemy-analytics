import { defineConfig } from "oxlint";
import core from "ultracite/oxlint/core";

export default defineConfig({
  extends: [core],
  ignorePatterns: [
    ...(core.ignorePatterns ?? []),
    ".agent_sources/**",
    ".agent-sources/**",
    "node_modules/**",
    ".brain/**",
    ".pi/**",
    "**/dist/**",
    // scripts/consumer-run.mjs runs in a temp project, so it has no local types to lint against.
    "scripts/**",
  ],
  options: { typeAware: true },
  rules: {
    "unicorn/throw-new-error": "off",
  },
});
