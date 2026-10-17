import { readFileSync } from "node:fs";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

const thirdPartyNotices: Plugin = {
  name: "wispcat-third-party-notices",
  generateBundle() {
    const source = ["react", "react-dom", "scheduler"]
      .map(
        (name) =>
          `${name}\n${"=".repeat(name.length)}\n\n${readFileSync(new URL(`./node_modules/${name}/LICENSE`, import.meta.url), "utf8")}`,
      )
      .join("\n\n");
    this.emitFile({
      type: "asset",
      fileName: "THIRD_PARTY_NOTICES.txt",
      source,
    });
  },
};
export default defineConfig({
  plugins: [react(), thirdPartyNotices],
  build: {
    outDir: "../internal/host/web/dist",
    emptyOutDir: true,
    sourcemap: false,
  },
  server: { host: "127.0.0.1" },
});
