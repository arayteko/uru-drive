let selectedFile: File | null = null;
const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });

const dropzone = document.getElementById('dropzone') as HTMLElement;
const fileInput = document.getElementById('fileInput') as HTMLInputElement;
const destPathInput = document.getElementById('destPath') as HTMLInputElement;
const startUploadBtn = document.getElementById('startUploadBtn') as HTMLButtonElement;
const serverUrlInput = document.getElementById('serverUrl') as HTMLInputElement;
const apiKeyInput = document.getElementById('apiKey') as HTMLInputElement;

const progressBar = document.getElementById('progressBar') as HTMLElement;
const progressStatus = document.getElementById('progressStatus') as HTMLElement;
const progressPercent = document.getElementById('progressPercent') as HTMLElement;
const logBox = document.getElementById('logBox') as HTMLElement;

const metricTotalSize = document.getElementById('metricTotalSize') as HTMLElement;
const metricChunksCount = document.getElementById('metricChunksCount') as HTMLElement;
const metricDedupRatio = document.getElementById('metricDedupRatio') as HTMLElement;
const metricSkippedCount = document.getElementById('metricSkippedCount') as HTMLElement;
const metricSavedBytes = document.getElementById('metricSavedBytes') as HTMLElement;
const metricSpeed = document.getElementById('metricSpeed') as HTMLElement;
const metricDuration = document.getElementById('metricDuration') as HTMLElement;

const filesTableBody = document.getElementById('filesTableBody') as HTMLElement;
const refreshFilesBtn = document.getElementById('refreshFilesBtn') as HTMLButtonElement;
const connectBtn = document.getElementById('connectBtn') as HTMLButtonElement;

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function appendLog(message: string, type: 'info' | 'success' | 'error' = 'info') {
  const line = document.createElement('div');
  line.className = `log-line ${type}`;
  const timestamp = new Date().toLocaleTimeString();
  line.textContent = `[${timestamp}] ${message}`;
  logBox.appendChild(line);
  logBox.scrollTop = logBox.scrollHeight;
}

// Drag & drop
dropzone.addEventListener('click', () => fileInput.click());
dropzone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropzone.classList.add('dragover');
});
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
dropzone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropzone.classList.remove('dragover');
  if (e.dataTransfer && e.dataTransfer.files.length > 0) {
    handleFileSelected(e.dataTransfer.files[0]);
  }
});

fileInput.addEventListener('change', () => {
  if (fileInput.files && fileInput.files.length > 0) {
    handleFileSelected(fileInput.files[0]);
  }
});

function handleFileSelected(file: File) {
  selectedFile = file;
  destPathInput.value = `/${file.name}`;
  appendLog(`Selected file: ${file.name} (${formatBytes(file.size)})`, 'info');
}

// Start upload
startUploadBtn.addEventListener('click', () => {
  if (!selectedFile) {
    alert('Please select a file first.');
    return;
  }

  const remotePath = destPathInput.value.trim() || `/${selectedFile.name}`;
  const serverUrl = serverUrlInput.value.trim();
  const apiKey = apiKeyInput.value.trim();

  startUploadBtn.disabled = true;
  progressBar.style.width = '0%';
  progressPercent.textContent = '0%';
  progressStatus.textContent = 'Initializing FastCDC WebWorker...';

  worker.postMessage({
    action: 'upload',
    file: selectedFile,
    remotePath,
    serverUrl,
    apiKey,
  });
});

// Worker messages
worker.onmessage = (e) => {
  const data = e.data;
  if (data.type === 'log') {
    appendLog(data.message, 'info');
  } else if (data.type === 'progress') {
    progressBar.style.width = `${Math.min(data.percent, 100)}%`;
    progressPercent.textContent = `${Math.round(data.percent)}%`;
    progressStatus.textContent = data.message;
  } else if (data.type === 'metrics') {
    metricTotalSize.textContent = formatBytes(data.totalSize);
    metricChunksCount.textContent = `${data.chunksDiscovered} chunks total`;
    metricDedupRatio.textContent = `${data.dedupRatio.toFixed(1)}%`;
    metricSkippedCount.textContent = `${data.chunksSkipped} chunks deduplicated`;
    metricSavedBytes.textContent = formatBytes(data.savedBytes);
  } else if (data.type === 'done') {
    progressBar.style.width = '100%';
    progressPercent.textContent = '100%';
    progressStatus.textContent = 'Completed!';
    startUploadBtn.disabled = false;

    metricSpeed.textContent = `${data.speedMBps.toFixed(2)} MB/s`;
    metricDuration.textContent = `Elapsed: ${data.elapsedSec.toFixed(2)}s`;

    appendLog(data.message, 'success');
    refreshFiles();
  } else if (data.type === 'error') {
    appendLog(`Error: ${data.error}`, 'error');
    progressStatus.textContent = 'Upload failed';
    startUploadBtn.disabled = false;
  }
};

// VFS Files table
async function refreshFiles() {
  const baseUrl = serverUrlInput.value.trim().replace(/\/+$/, '');
  const apiKey = apiKeyInput.value.trim();

  try {
    const res = await fetch(`${baseUrl}/api/v1/files?prefix=/`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });

    if (!res.ok) {
      throw new Error(await res.text());
    }

    const files: Array<[string, any]> = await res.json();
    filesTableBody.innerHTML = '';

    if (files.length === 0) {
      filesTableBody.innerHTML = `
        <tr>
          <td colspan="4" style="text-align: center; color: var(--text-muted); padding: 20px;">
            No files in private namespace.
          </td>
        </tr>`;
      return;
    }

    for (const [path, ver] of files) {
      const tr = document.createElement('tr');
      const sizeStr = ver.manifest ? formatBytes(ver.manifest.size) : 'Deleted';
      const versionId = ver.version_id;

      tr.innerHTML = `
        <td><strong>${path}</strong></td>
        <td>${sizeStr}</td>
        <td style="font-family: monospace; font-size: 11px; color: var(--text-muted);">${versionId}</td>
        <td class="actions">
          <button class="btn-outline" style="font-size: 11px; padding: 4px 8px;" data-action="download" data-path="${path}">Download</button>
          <button class="btn-outline" style="font-size: 11px; padding: 4px 8px;" data-action="history" data-path="${path}">History</button>
          <button class="btn-danger" style="font-size: 11px; padding: 4px 8px;" data-action="delete" data-path="${path}">Delete</button>
        </td>
      `;
      filesTableBody.appendChild(tr);
    }
  } catch (err: any) {
    appendLog(`Failed to fetch file list: ${err.message || err}`, 'error');
  }
}

filesTableBody.addEventListener('click', async (e) => {
  const target = e.target as HTMLElement;
  const action = target.getAttribute('data-action');
  const path = target.getAttribute('data-path');
  if (!action || !path) return;

  const baseUrl = serverUrlInput.value.trim().replace(/\/+$/, '');
  const apiKey = apiKeyInput.value.trim();

  if (action === 'download') {
    const downloadUrl = `${baseUrl}/api/v1/files${path}?api_key=${encodeURIComponent(apiKey)}`;
    window.open(downloadUrl, '_blank');
  } else if (action === 'delete') {
    if (confirm(`Are you sure you want to delete ${path}?`)) {
      const res = await fetch(`${baseUrl}/api/v1/files${path}`, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${apiKey}` },
      });
      if (res.ok) {
        appendLog(`Deleted ${path}`, 'success');
        refreshFiles();
      } else {
        appendLog(`Delete failed: ${await res.text()}`, 'error');
      }
    }
  } else if (action === 'history') {
    const res = await fetch(`${baseUrl}/api/v1/files-history${path}`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });
    if (res.ok) {
      const versions = await res.json();
      let msg = `Version history for ${path}:\n\n`;
      for (const v of versions) {
        msg += `Version: ${v.version_id}\nDeleted: ${v.is_delete_marker}\nCreated: ${new Date(v.created_at_ms).toLocaleString()}\nSize: ${v.manifest ? formatBytes(v.manifest.size) : '0 B'}\n\n`;
      }
      alert(msg);
    } else {
      appendLog(`History query failed: ${await res.text()}`, 'error');
    }
  }
});

refreshFilesBtn.addEventListener('click', refreshFiles);
connectBtn.addEventListener('click', refreshFiles);

// Initial load
appendLog('Initialized Web Client frontend. Ready to connect to UruDrive server.', 'info');
