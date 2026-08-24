"use strict";
const $ = (id) => document.getElementById(id);

function fmtBytes(b) {
  if (b < 1024) return b + " B";
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KiB";
  return (b / 1048576).toFixed(1) + " MiB";
}
function fmtInt(n) { return n.toLocaleString(); }

// --- throughput history ---
const MAXPTS = 80;
let writeHist = [], readHist = [];
let prev = null;  // { t, reads, writes }

function drawChart(canvas, data, color) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  if (data.length < 2) return;
  const max = Math.max(1, ...data);
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = (i / (MAXPTS - 1)) * w;
    const y = h - (v / max) * (h - 4) - 2;
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.stroke();
  // soft fill
  ctx.lineTo((data.length - 1) / (MAXPTS - 1) * w, h);
  ctx.lineTo(0, h); ctx.closePath();
  ctx.fillStyle = color + "22"; ctx.fill();
}

function renderTiers(sstables) {
  const host = $("tiers");
  $("tiers-empty").style.display = sstables.length ? "none" : "block";
  // group by tier
  const byTier = new Map();
  let maxSize = 1;
  for (const s of sstables) {
    if (!byTier.has(s.tier)) byTier.set(s.tier, []);
    byTier.get(s.tier).push(s);
    maxSize = Math.max(maxSize, s.size);
  }
  const tiers = [...byTier.keys()].sort((a, b) => a - b); // small -> large
  host.innerHTML = "";
  tiers.forEach((tier, idx) => {
    const row = document.createElement("div");
    row.className = "tier";
    const name = document.createElement("div");
    name.className = "name"; name.textContent = "L" + idx;
    const boxes = document.createElement("div");
    boxes.className = "boxes";
    for (const s of byTier.get(tier)) {
      const el = document.createElement("div");
      el.className = "sst";
      el.style.width = (44 + (s.size / maxSize) * 200) + "px";
      const range = s.min === s.max ? s.min : (s.min + " .. " + s.max);
      el.innerHTML = `<div class="n">${fmtInt(s.records)} keys</div>` +
                     `<div class="r">${fmtBytes(s.size)} &middot; ${range}</div>`;
      boxes.appendChild(el);
    }
    row.appendChild(name); row.appendChild(boxes);
    host.appendChild(row);
  });
}

function renderMemtable(m) {
  const pct = m.threshold ? Math.min(100, (m.bytes / m.threshold) * 100) : 0;
  $("mem-fill").style.width = pct + "%";
  $("mem-left").textContent = fmtInt(m.entries) + " entries";
  $("mem-right").textContent = fmtBytes(m.bytes) + " / " + fmtBytes(m.threshold);
}

function renderCounters(d) {
  $("s-writes").textContent = fmtInt(d.counters.writes);
  $("s-reads").textContent = fmtInt(d.counters.reads);
  $("s-deletes").textContent = fmtInt(d.counters.deletes);
  $("s-compactions").textContent = fmtInt(d.counters.compactions);
  $("s-tomb").textContent = fmtInt(d.memtable.tombstones);
  $("s-disk").textContent = fmtBytes(d.disk);
  const bc = d.bloom.checks;
  $("s-bloom").textContent = bc ? ((d.bloom.skips / bc) * 100).toFixed(1) + "%" : "—";
  const u = d.writeAmp.user;
  const total = d.writeAmp.wal + d.writeAmp.flush + d.writeAmp.compaction;
  $("s-wa").textContent = u ? (total / u).toFixed(2) + "×" : "—";
}

async function poll() {
  let d;
  try {
    d = await (await fetch("/stats")).json();
  } catch (e) { return; }

  renderTiers(d.sstables);
  renderMemtable(d.memtable);
  renderCounters(d);
  renderWorkload(d.workload);

  const now = performance.now();
  if (prev) {
    const dt = (now - prev.t) / 1000;
    if (dt > 0) {
      writeHist.push((d.counters.writes - prev.writes) / dt);
      readHist.push((d.counters.reads - prev.reads) / dt);
      if (writeHist.length > MAXPTS) writeHist.shift();
      if (readHist.length > MAXPTS) readHist.shift();
    }
  }
  prev = { t: now, reads: d.counters.reads, writes: d.counters.writes };
  drawChart($("c-write"), writeHist, "#60a5fa");
  drawChart($("c-read"), readHist, "#34d399");
  $("l-write").textContent = fmtInt(Math.round(writeHist.at(-1) || 0)) + " /s";
  $("l-read").textContent = fmtInt(Math.round(readHist.at(-1) || 0)) + " /s";
}

function renderWorkload(wl) {
  const st = $("status"), rs = $("run-sync"), rg = $("run-group");
  if (wl && wl.running) {
    st.textContent = "running: " + (wl.group ? "group commit" : "fsync / write");
    st.classList.add("live");
    rs.disabled = true; rg.disabled = true;
  } else {
    st.textContent = "idle"; st.classList.remove("live");
    rs.disabled = false; rg.disabled = false;
  }
}

$("run-sync").addEventListener("click", () =>
  fetch("/workload?mode=sync", { method: "POST" }));
$("run-group").addEventListener("click", () =>
  fetch("/workload?mode=group", { method: "POST" }));

setInterval(poll, 500);
poll();
