// vite.config.mjs
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import jsconfigPaths from 'vite-jsconfig-paths';
import path from 'path';
import { fileURLToPath } from 'url';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const API_URL = `${'/'}`;
  const PORT = 3000;

  return {
    server: {
      open: true,
      port: PORT,
      proxy: {
        // ★ SSE 전용: 정규식으로 "정확히" 이 경로만 잡음
        [/^\/api\/account\/summary\/stream$/]: {
          target: 'http://127.0.0.1:5000',
          changeOrigin: true,
          secure: false,
          ws: false,                         // SSE는 WebSocket 아님
          timeout: 0,                        // 클라이언트 타임아웃 해제
          proxyTimeout: 0,                   // 백엔드 응답 타임아웃 해제
          agent: new http.Agent({ keepAlive: true, keepAliveMsecs: 60000, maxSockets: 100 }),
          headers: {
            Connection: 'keep-alive',
            Accept: 'text/event-stream',
            'Cache-Control': 'no-cache',
          },
          configure(proxy) {
            // 요청 시 gzip 비활성화(버퍼링 방지)
            proxy.on('proxyReq', (proxyReq) => {
              try {
                proxyReq.removeHeader('accept-encoding');
              } catch {}
              proxyReq.setHeader('cache-control', 'no-cache');
            });
            // 응답 헤더도 강제 세팅
            proxy.on('proxyRes', (proxyRes) => {
              if (proxyRes && proxyRes.headers) {
                proxyRes.headers['x-accel-buffering'] = 'no';
                proxyRes.headers['cache-control'] = 'no-cache';
                proxyRes.headers['connection'] = 'keep-alive';
                // 일부 환경에서 도움됨: no-transform
                proxyRes.headers['surrogate-control'] = 'no-store';
              }
            });
            proxy.on('error', (err) => {
              console.error('[SSE proxy error]', err?.code || err?.message);
            });
            proxy.on('close', () => {
              // console.log('[SSE proxy] client disconnected');
            });
          },
        },

        // 나머지 API
        '/api': {
          target: 'http://127.0.0.1:5000',
          changeOrigin: true,
          secure: false,
        },
      },
    },
    define: { global: 'window' },
    resolve: { alias: [{ find: '@', replacement: path.resolve(__dirname, 'src') }] },
    css: {
      preprocessorOptions: { scss: { charset: false }, less: { charset: false } },
      charset: false,
      postcss: {
        plugins: [{
          postcssPlugin: 'internal:charset-removal',
          AtRule: { charset: (atRule) => { if (atRule.name === 'charset') atRule.remove(); } },
        }],
      },
    },
    base: API_URL,
    plugins: [react(), jsconfigPaths()],
  };
});
