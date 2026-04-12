import { useEffect, useRef, useState } from "react";

/**
 * 按固定间隔执行刷新；同刻仅允许一次请求在途，避免堆积。
 * @param {object} p
 * @param {() => void | Promise<void>} p.tickFn - 每次触发的异步逻辑（请用 ref 保持最新闭包）
 * @param {boolean} p.enabled
 * @param {number} [p.intervalMs=1500]
 */
export function useAutoRefresh({ tickFn, enabled, intervalMs = 1500 }) {
  const tickRef = useRef(tickFn);
  tickRef.current = tickFn;

  const [lastUpdatedAt, setLastUpdatedAt] = useState(null);
  const [isTicking, setIsTicking] = useState(false);
  const inFlightRef = useRef(false);

  useEffect(() => {
    if (!enabled) return undefined;

    const run = async () => {
      if (inFlightRef.current) return;
      inFlightRef.current = true;
      setIsTicking(true);
      try {
        await tickRef.current();
        setLastUpdatedAt(new Date());
      } catch {
        /* 具体错误由 tick 内处理 */
      } finally {
        inFlightRef.current = false;
        requestAnimationFrame(() => setIsTicking(false));
      }
    };

    void run();
    const id = window.setInterval(() => void run(), intervalMs);
    return () => window.clearInterval(id);
  }, [enabled, intervalMs]);

  return { lastUpdatedAt, isTicking };
}
