:root {
  --bg-1: #06122b;
  --bg-2: #0b1d45;
  --bg-3: #10285d;
  --card: rgba(18, 35, 78, 0.72);
  --card-strong: rgba(21, 42, 94, 0.9);
  --stroke: rgba(255, 255, 255, 0.08);
  --stroke-strong: rgba(255, 255, 255, 0.16);
  --text: #f4f8ff;
  --text-soft: #b7c6e6;
  --text-dim: #8fa3cc;
  --primary: #65c8ff;
  --primary-2: #8a7dff;
  --success: #2dd4bf;
  --warning: #fbbf24;
  --danger: #fb7185;
  --shadow-lg: 0 22px 60px rgba(0, 0, 0, 0.35);
  --shadow-md: 0 14px 34px rgba(0, 0, 0, 0.24);
  --radius-xl: 28px;
  --radius-lg: 22px;
  --radius-md: 16px;
  --radius-sm: 12px;
}

* {
  box-sizing: border-box;
}

html {
  scroll-behavior: smooth;
}

body {
  margin: 0;
  min-height: 100vh;
  color: var(--text);
  font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background:
    radial-gradient(circle at top left, rgba(101, 200, 255, 0.14), transparent 24%),
    radial-gradient(circle at top right, rgba(138, 125, 255, 0.18), transparent 28%),
    linear-gradient(135deg, var(--bg-1) 0%, var(--bg-2) 48%, var(--bg-3) 100%);
  background-attachment: fixed;
  overflow-x: hidden;
}

body::before,
body::after {
  content: "";
  position: fixed;
  inset: auto;
  width: 340px;
  height: 340px;
  border-radius: 999px;
  filter: blur(90px);
  pointer-events: none;
  z-index: 0;
  opacity: 0.32;
  animation: floatGlow 14s ease-in-out infinite;
}

body::before {
  left: -100px;
  top: 80px;
  background: rgba(101, 200, 255, 0.38);
}

body::after {
  right: -80px;
  bottom: 60px;
  background: rgba(138, 125, 255, 0.28);
  animation-delay: -7s;
}

#app {
  position: relative;
  z-index: 1;
  max-width: 1280px;
  margin: 0 auto;
  padding: 26px 18px 80px;
}

.loading,
.error-box,
.empty-state,
.meta,
.filters,
.history,
.card {
  animation: fadeUp 0.5s ease both;
}

.loading {
  margin-top: 30px;
  padding: 26px 20px;
  text-align: center;
  font-size: 1.05rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  color: var(--text);
  border: 1px solid var(--stroke);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
  backdrop-filter: blur(14px);
  box-shadow: var(--shadow-md);
  position: relative;
  overflow: hidden;
}

.loading::before {
  content: "";
  position: absolute;
  inset: 0;
  transform: translateX(-100%);
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent);
  animation: shimmer 1.5s infinite;
}

.filters {
  display: grid;
  grid-template-columns: 1.2fr 1fr 180px;
  gap: 14px;
  margin-bottom: 14px;
  padding: 16px;
  border-radius: var(--radius-xl);
  background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.035));
  border: 1px solid var(--stroke);
  backdrop-filter: blur(16px);
  box-shadow: var(--shadow-lg);
}

.filters select,
.filters button {
  width: 100%;
  height: 54px;
  border-radius: 18px;
  border: 1px solid var(--stroke-strong);
  outline: none;
  font-size: 1rem;
  font-weight: 700;
  transition: transform 0.2s ease, box-shadow 0.25s ease, border-color 0.25s ease, background 0.25s ease;
}

.filters select {
  padding: 0 16px;
  color: var(--text);
  background:
    linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
  backdrop-filter: blur(10px);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
}

.filters select:focus,
.filters select:hover {
  border-color: rgba(101, 200, 255, 0.5);
  box-shadow: 0 0 0 4px rgba(101, 200, 255, 0.08);
  transform: translateY(-1px);
}

.filters button {
  cursor: pointer;
  color: #08152d;
  background: linear-gradient(135deg, var(--primary), #7ee7ff);
  box-shadow: 0 12px 28px rgba(101, 200, 255, 0.3);
}

.filters button:hover {
  transform: translateY(-2px) scale(1.01);
  box-shadow: 0 18px 34px rgba(101, 200, 255, 0.38);
}

.filters button:active {
  transform: translateY(0) scale(0.99);
}

.meta {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 4px 0 18px;
  padding: 10px 14px;
  border-radius: 999px;
  font-size: 0.95rem;
  color: var(--text-soft);
  background: rgba(255,255,255,0.05);
  border: 1px solid var(--stroke);
  backdrop-filter: blur(12px);
}

.error-box {
  margin: 0 0 18px;
  padding: 16px 18px;
  border-radius: 18px;
  font-weight: 700;
  color: #ffe2e8;
  border: 1px solid rgba(251, 113, 133, 0.26);
  background: linear-gradient(180deg, rgba(120, 24, 52, 0.42), rgba(85, 18, 39, 0.26));
  box-shadow: var(--shadow-md);
}

.empty-state {
  padding: 22px 18px;
  border-radius: 20px;
  text-align: center;
  color: var(--text-soft);
  background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
  border: 1px dashed rgba(255,255,255,0.12);
}

.history {
  margin-top: 22px;
  padding: 22px;
  border-radius: 30px;
  background:
    linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.03));
  border: 1px solid var(--stroke);
  box-shadow: var(--shadow-lg);
  backdrop-filter: blur(18px);
  position: relative;
  overflow: hidden;
}

.history::before {
  content: "";
  position: absolute;
  inset: 0;
  background:
    linear-gradient(120deg, transparent 0%, rgba(255,255,255,0.035) 45%, transparent 60%);
  transform: translateX(-120%);
  animation: sectionShine 7s linear infinite;
  pointer-events: none;
}

.history h2 {
  margin: 0 0 18px;
  font-size: clamp(1.45rem, 2.4vw, 2rem);
  letter-spacing: -0.03em;
  color: var(--text);
}

.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 18px;
}

.card {
  position: relative;
  overflow: hidden;
  padding: 20px;
  border-radius: 26px;
  background:
    linear-gradient(180deg, rgba(26, 45, 95, 0.92), rgba(17, 33, 75, 0.82));
  border: 1px solid rgba(255,255,255,0.08);
  box-shadow: var(--shadow-lg);
  transition: transform 0.28s ease, box-shadow 0.28s ease, border-color 0.28s ease;
  isolation: isolate;
}

.card::before {
  content: "";
  position: absolute;
  inset: -1px;
  border-radius: inherit;
  padding: 1px;
  background: linear-gradient(135deg, rgba(101, 200, 255, 0.22), rgba(138, 125, 255, 0.18), transparent 55%);
  -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
  -webkit-mask-composite: xor;
          mask-composite: exclude;
  z-index: -1;
}

.card::after {
  content: "";
  position: absolute;
  width: 220px;
  height: 220px;
  right: -90px;
  top: -90px;
  background: radial-gradient(circle, rgba(101,200,255,0.13), transparent 60%);
  pointer-events: none;
}

.card:hover {
  transform: translateY(-6px);
  box-shadow: 0 26px 60px rgba(0, 0, 0, 0.42);
  border-color: rgba(101, 200, 255, 0.2);
}

.top {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: flex-start;
  margin-bottom: 14px;
}

.league {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
  font-size: 0.88rem;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: #7fb8ff;
}

.league::before {
  content: "";
  width: 12px;
  height: 12px;
  border-radius: 999px;
  background: linear-gradient(135deg, #8a7dff, #65c8ff);
  box-shadow: 0 0 12px rgba(138, 125, 255, 0.45);
}

.card h2 {
  margin: 0;
  font-size: clamp(1.45rem, 2.6vw, 2.2rem);
  line-height: 1.08;
  letter-spacing: -0.04em;
}

.top > div:last-child {
  min-width: fit-content;
  padding: 12px 16px;
  border-radius: 18px;
  color: var(--text);
  font-weight: 800;
  font-size: 0.98rem;
  background: rgba(255,255,255,0.05);
  border: 1px solid var(--stroke);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
}

.tags {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 16px;
}

.t,
.b,
.r {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 40px;
  padding: 9px 16px;
  border-radius: 999px;
  font-size: 0.95rem;
  font-weight: 800;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.05);
  color: var(--text);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
}

.b.green {
  color: #dcfff3;
  background: linear-gradient(135deg, rgba(16, 185, 129, 0.22), rgba(45, 212, 191, 0.16));
  border-color: rgba(45, 212, 191, 0.22);
}

.b.yellow {
  color: #fff4ce;
  background: linear-gradient(135deg, rgba(251, 191, 36, 0.22), rgba(245, 158, 11, 0.16));
  border-color: rgba(251, 191, 36, 0.2);
}

.b.red {
  color: #ffe3eb;
  background: linear-gradient(135deg, rgba(251, 113, 133, 0.2), rgba(225, 29, 72, 0.14));
  border-color: rgba(251, 113, 133, 0.18);
}

.r.win {
  color: #dcfff3;
  background: linear-gradient(135deg, rgba(16,185,129,0.2), rgba(45,212,191,0.16));
}

.r.lost {
  color: #ffe3eb;
  background: linear-gradient(135deg, rgba(251,113,133,0.2), rgba(225,29,72,0.14));
}

.r.pend {
  color: #fff4ce;
  background: linear-gradient(135deg, rgba(251,191,36,0.22), rgba(245,158,11,0.16));
}

.pick {
  margin-bottom: 16px;
  padding: 18px 20px;
  border-radius: 22px;
  font-size: clamp(1.2rem, 2.2vw, 1.75rem);
  font-weight: 900;
  letter-spacing: -0.04em;
  color: white;
  background:
    linear-gradient(135deg, rgba(60, 96, 184, 0.42), rgba(71, 66, 180, 0.3));
  border: 1px solid rgba(113, 159, 255, 0.18);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
  position: relative;
  overflow: hidden;
}

.pick::after {
  content: "";
  position: absolute;
  inset: 0;
  background: linear-gradient(120deg, transparent 0%, rgba(255,255,255,0.06) 40%, transparent 62%);
  transform: translateX(-110%);
  animation: shimmer 3.2s infinite;
}

.stats {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 12px;
}

.stats span {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 40px;
  padding: 10px 14px;
  border-radius: 18px;
  color: var(--text-soft);
  font-weight: 700;
  background: rgba(255,255,255,0.045);
  border: 1px solid rgba(255,255,255,0.07);
}

.score {
  margin: 10px 0 0;
  padding: 12px 14px;
  border-radius: 16px;
  font-weight: 800;
  color: #dff5ff;
  background: rgba(101,200,255,0.08);
  border: 1px solid rgba(101,200,255,0.14);
}

.tip {
  margin: 16px 0 0;
  color: var(--text-soft);
  font-size: 1rem;
  line-height: 1.65;
}

.day {
  padding: 18px;
  border-radius: 24px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.07);
}

.day + .day {
  margin-top: 14px;
}

.day h3 {
  margin: 0 0 14px;
  font-size: 1.15rem;
  color: var(--text);
}

.day-stats {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 14px;
}

.day-stats span {
  padding: 9px 14px;
  border-radius: 999px;
  font-size: 0.95rem;
  font-weight: 800;
  color: var(--text);
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.07);
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.history-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 14px;
  padding: 16px 18px;
  border-radius: 18px;
  background: linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.025));
  border: 1px solid rgba(255,255,255,0.07);
  transition: transform 0.2s ease, border-color 0.2s ease, background 0.2s ease;
}

.history-row:hover {
  transform: translateY(-2px);
  border-color: rgba(101, 200, 255, 0.18);
  background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
}

.history-row-left {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.history-row-left strong {
  color: var(--text);
  font-size: 1.02rem;
}

.history-row-left span {
  color: var(--text-soft);
  font-weight: 700;
}

.history-row-left small {
  color: var(--text-dim);
  font-size: 0.88rem;
  line-height: 1.45;
}

.history-row-right {
  flex-shrink: 0;
}

@media (max-width: 980px) {
  .filters {
    grid-template-columns: 1fr;
  }

  #app {
    padding: 18px 14px 64px;
  }

  .history {
    padding: 18px;
    border-radius: 24px;
  }

  .card {
    padding: 18px;
    border-radius: 22px;
  }
}

@media (max-width: 640px) {
  .top {
    flex-direction: column;
    align-items: flex-start;
  }

  .top > div:last-child {
    width: 100%;
  }

  .pick {
    font-size: 1.15rem;
  }

  .history-row {
    flex-direction: column;
    align-items: flex-start;
  }

  .history-row-right {
    align-self: flex-end;
  }

  .stats span,
  .t,
  .b,
  .r {
    min-height: 38px;
    font-size: 0.9rem;
  }
}

@keyframes fadeUp {
  from {
    opacity: 0;
    transform: translateY(14px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes shimmer {
  100% {
    transform: translateX(110%);
  }
}

@keyframes sectionShine {
  100% {
    transform: translateX(140%);
  }
}

@keyframes floatGlow {
  0%, 100% {
    transform: translateY(0px) scale(1);
  }
  50% {
    transform: translateY(-18px) scale(1.05);
  }
}