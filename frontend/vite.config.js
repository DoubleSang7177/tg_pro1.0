import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    /** 开发时把 /api 转到本机 FastAPI，避免用局域网 IP 打开页面时直连 localhost:8000 触发 CORS / Failed to fetch */
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
        ws: true,
        rewrite: (p) => p.replace(/^\/api/, "") || "/",
      },
    },
  },
});
