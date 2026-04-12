import { useMemo } from "react";

/** SESSION.LOG 专用：慢速漂移粒子（40–60），仅视觉层 */
const PARTICLE_COUNT = 52;

export function SessionLogParticleBackdrop() {
  const particles = useMemo(
    () =>
      Array.from({ length: PARTICLE_COUNT }, (_, i) => ({
        id: i,
        left: `${2 + Math.random() * 96}%`,
        top: `${2 + Math.random() * 96}%`,
        size: 1 + Math.random() * 2,
        durationSec: 26 + Math.random() * 28,
        delaySec: -(Math.random() * 45),
        dx: `${8 + Math.random() * 40}px`,
        dy: `${-(18 + Math.random() * 50)}px`,
      })),
    [],
  );

  return (
    <div className="session-log-particles" aria-hidden>
      {particles.map((p) => (
        <span
          key={p.id}
          className="session-log-particle"
          style={{
            left: p.left,
            top: p.top,
            width: p.size,
            height: p.size,
            animationDuration: `${p.durationSec}s`,
            animationDelay: `${p.delaySec}s`,
            "--session-log-p-dx": p.dx,
            "--session-log-p-dy": p.dy,
          }}
        />
      ))}
    </div>
  );
}
