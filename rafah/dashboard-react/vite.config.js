import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      "/news-api": {
        target: "http://100.118.244.91:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/news-api/, ""),
      },
      "/kurs-api": {
        target: "http://100.118.244.91:8002",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/kurs-api/, ""),
      },
      "/commodity-api": {
        target: "http://100.92.242.101:8001",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/commodity-api/, ""),
      },
    },
  },
});
