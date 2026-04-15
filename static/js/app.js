/* app.js — MediaWatcher frontend */

// Derive base path from the current page URL so the app works
// whether served at / or a subpath like /mediawatcher
const BASE = (() => {
  // Strip trailing slash, then remove known page suffixes to get the root
  const p = window.location.pathname
    .replace(/\/job\/\d+\/?$/, '')
    .replace(/\/(config|stream)\/?$/, '')
    .replace(/\/$/, '');
  return p;
})();

// ── SSE connection ──────────────────────────────────────────────────────────

const liveBadge   = document.getElementById('live-badge');
const liveLog     = document.getElementById('live-log');
const connDot     = document.getElementById('conn-dot');
const connLabel   = document.getElementById('conn-label');
const currentFile = document.getElementById('current-file');
const autoscroll  = document.getElementById('autoscroll');

let activeJobId   = null;
let es            = null;

function setConnected(ok) {
  if (!connDot) return;
  connDot.className  = 'status-dot ' + (ok ? 'connected' : 'error');
  connLabel.textContent = ok ? 'Connected' : 'Disconnected';
}

function appendLiveLine(line, kind) {
  if (!liveLog) return;
  const placeholder = liveLog.querySelector('.log-placeholder');
  if (placeholder) placeholder.remove();

  const div = document.createElement('div');
  div.className = 'log-line'
    + (kind === 'error' || line.includes('✗') ? ' line-error' : '')
    + (kind === 'ok'    || line.includes('✓') ? ' line-ok'    : '')
    + (line.startsWith('──') || line.startsWith('==') ? ' line-header' : '');
  div.textContent = line;
  liveLog.appendChild(div);

  // Trim buffer to last 2000 lines
  while (liveLog.children.length > 2000) {
    liveLog.removeChild(liveLog.firstChild);
  }

  if (autoscroll && autoscroll.checked) {
    liveLog.scrollTop = liveLog.scrollHeight;
  }
}

function setLiveBadge(running, label) {
  if (!liveBadge) return;
  liveBadge.textContent = label || (running ? 'LIVE' : 'IDLE');
  if (running) liveBadge.classList.add('active');
  else         liveBadge.classList.remove('active');
}

function connectSSE() {
  if (es) { try { es.close(); } catch(_) {} }

  es = new EventSource(`${BASE}/stream`);

  es.onopen = () => {
    setConnected(true);
  };

  es.onmessage = (event) => {
    let evt;
    try { evt = JSON.parse(event.data); } catch(_) { return; }

    switch (evt.type) {

      case 'job_queued':
        appendLiveLine(`⏳ Queued: ${shortPath(evt.file)}  [job #${evt.job_id}]`, 'info');
        // Refresh immediately then again shortly after so cancel button appears
        refreshJobs();
        setTimeout(refreshJobs, 300);
        break;

      case 'step_start':
      case 'separator':
      case 'info':
      case 'log':
      case 'ok':
      case 'error': {
        const line = evt.line || '';
        if (evt.job_id !== activeJobId) {
          activeJobId = evt.job_id;
          setLiveBadge(true, 'LIVE');
          if (currentFile) currentFile.textContent = shortPath(evt.file);
        }
        appendLiveLine(line, evt.type);
        break;
      }

      case 'job_done':
        setLiveBadge(false, 'IDLE');
        if (currentFile) currentFile.textContent = '';
        activeJobId = null;
        refreshJobs();
        break;
    }
  };

  es.onerror = () => {
    setConnected(false);
    setLiveBadge(false, 'IDLE');
    // Reconnect after 3s
    setTimeout(connectSSE, 3000);
  };
}

function shortPath(p) {
  if (!p) return '';
  const parts = p.replace(/\\/g, '/').split('/');
  return parts[parts.length - 1] || p;
}

// ── Job table refresh ───────────────────────────────────────────────────────

async function refreshJobs() {
  try {
    const resp = await fetch(`${BASE}/api/jobs`);
    const jobs = await resp.json();
    const wrap = document.getElementById('jobs-table-wrap');
    if (!wrap) return;
    renderJobsTable(jobs, wrap);
  } catch(e) {
    console.warn('refreshJobs failed:', e);
  }
}

function renderJobsTable(jobs, container) {
  if (!jobs || jobs.length === 0) {
    container.innerHTML = '<div class="empty-state">No jobs yet. Drop an MKV into a watched directory to get started.</div>';
    return;
  }

  let html = `<table class="jobs-table">
    <thead>
      <tr>
        <th class="col-hide-sm">#</th><th>File</th><th>Status</th>
        <th>Steps</th><th class="col-hide-sm">Queued</th><th class="col-hide-sm">Duration</th><th></th>
      </tr>
    </thead>
    <tbody>`;

  for (const job of jobs) {
    const fname = shortPath(job.file_path);
    const queued = (job.queued_at || '').replace('T', ' ').replace('Z', '');
    const pct = job.total_steps > 0
      ? Math.round((job.current_step / job.total_steps) * 100) : 0;

    const dur = formatDuration(job.started_at, job.finished_at);

    let badgeInner = '';
    if (job.status === 'running')       badgeInner = '<span class="spinner"></span> running';
    else if (job.status === 'done')        badgeInner = '✓ done';
    else if (job.status === 'failed')      badgeInner = '✗ failed';
    else if (job.status === 'interrupted') badgeInner = '⏸ interrupted';
    else if (job.status === 'cancelled')   badgeInner = '✕ cancelled';
    else                                   badgeInner = '⏳ queued';

    html += `<tr class="job-row job-${job.status}">
      <td class="job-id col-hide-sm">${job.id}</td>
      <td class="job-file" title="${job.file_path}">${escHtml(fname)}</td>
      <td class="job-status"><span class="badge badge-${job.status}">${badgeInner}</span></td>
      <td class="job-steps">
        ${job.total_steps > 0
          ? `${job.current_step}/${job.total_steps}<div class="step-bar"><div class="step-fill" style="width:${pct}%"></div></div>`
          : '—'}
      </td>
      <td class="job-time col-hide-sm">${escHtml(queued)}</td>
      <td class="job-dur col-hide-sm">${dur}</td>
      <td style="white-space:nowrap">
        <a href="${BASE}/job/${job.id}" class="btn-ghost btn-sm">Logs</a>
        ${(job.status !== 'running' && job.status !== 'queued')
          ? `<button class="btn-ghost btn-sm" onclick="rerunJob(${job.id}, this)">↻</button>`
          : ''}
        ${job.status === 'queued'
          ? `<button class="btn-ghost btn-sm btn-cancel" onclick="cancelJob(${job.id}, this)">✕</button>`
          : ''}
      </td>
    </tr>`;
  }

  html += '</tbody></table>';
  container.innerHTML = html;
}

function formatDuration(start, end) {
  if (!start) return '—';
  const s = new Date(start);
  const e = end ? new Date(end) : new Date();
  const sec = Math.round((e - s) / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60), r = sec % 60;
  return `${m}m ${r}s`;
}

function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Clear live log ──────────────────────────────────────────────────────────

function clearLive() {
  if (!liveLog) return;
  liveLog.innerHTML = '<div class="log-placeholder">Cleared — waiting for next pipeline run…</div>';
}

// ── Running timers (duration tick) ──────────────────────────────────────────

function tickRunningTimers() {
  document.querySelectorAll('.running-timer').forEach(el => {
    const start = new Date(el.dataset.start);
    const sec = Math.round((Date.now() - start) / 1000);
    el.textContent = formatDuration(el.dataset.start, null);
  });
}

// ── Auto-refresh jobs table while anything is running ───────────────────────

function startAutoRefresh() {
  setInterval(() => {
    const hasActive = document.querySelector('.badge-running, .badge-queued');
    if (hasActive) refreshJobs();
    tickRunningTimers();
  }, 1000);
}

// ── Cancel ──────────────────────────────────────────────────────────────────

async function cancelJob(jobId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try {
    const resp = await fetch(`${BASE}/api/job/${jobId}/cancel`, { method: 'POST' });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.disabled = false; btn.textContent = '✕'; }
    } else {
      refreshJobs();
    }
  } catch(e) {
    alert('Request failed: ' + e);
    if (btn) { btn.disabled = false; btn.textContent = '✕'; }
  }
}

// ── Re-run ──────────────────────────────────────────────────────────────────

async function rerunJob(jobId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try {
    const resp = await fetch(`${BASE}/api/job/${jobId}/rerun`, { method: 'POST' });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.disabled = false; btn.textContent = '↻'; }
    } else {
      refreshJobs();
    }
  } catch(e) {
    alert('Request failed: ' + e);
    if (btn) { btn.disabled = false; btn.textContent = '↻'; }
  }
}

// ── Resize handle ───────────────────────────────────────────────────────────

function initResizeHandle() {
  const handle = document.getElementById('resize-handle');
  const livePanel = document.querySelector('.live-panel');
  if (!handle || !livePanel) return;

  // Restore saved height
  const savedHeight = localStorage.getItem('livePanelHeight');
  if (savedHeight) livePanel.style.flex = `0 0 ${savedHeight}px`;

  let startY = 0;
  let startHeight = 0;

  function onMouseMove(e) {
    const delta = e.clientY - startY;
    const newHeight = Math.max(80, Math.min(startHeight + delta, window.innerHeight * 0.8));
    livePanel.style.flex = `0 0 ${newHeight}px`;
  }

  function onMouseUp() {
    handle.classList.remove('dragging');
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
    document.body.style.cursor = '';
    // Save height
    const h = parseInt(livePanel.style.flex.split(' ').pop());
    if (!isNaN(h)) localStorage.setItem('livePanelHeight', h);
  }

  handle.addEventListener('mousedown', (e) => {
    e.preventDefault();
    startY = e.clientY;
    startHeight = livePanel.getBoundingClientRect().height;
    handle.classList.add('dragging');
    document.body.style.cursor = 'ns-resize';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  });

  // Touch support
  handle.addEventListener('touchstart', (e) => {
    e.preventDefault();
    startY = e.touches[0].clientY;
    startHeight = livePanel.getBoundingClientRect().height;
    handle.classList.add('dragging');
  }, { passive: false });

  handle.addEventListener('touchmove', (e) => {
    e.preventDefault();
    const delta = e.touches[0].clientY - startY;
    const newHeight = Math.max(80, Math.min(startHeight + delta, window.innerHeight * 0.8));
    livePanel.style.flex = `0 0 ${newHeight}px`;
  }, { passive: false });

  handle.addEventListener('touchend', () => {
    handle.classList.remove('dragging');
    const h = parseInt(livePanel.style.flex.split(' ').pop());
    if (!isNaN(h)) localStorage.setItem('livePanelHeight', h);
  });
}

// ── Init ────────────────────────────────────────────────────────────────────

connectSSE();
startAutoRefresh();
initResizeHandle();
