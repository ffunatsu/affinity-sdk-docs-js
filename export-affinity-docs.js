const fs = require('fs');
const path = require('path');

const SERVER_URL = 'http://localhost:6767/sse';
const OUTPUT_DIR = path.join(__dirname, 'docs');

async function main() {
  console.log('Connecting to Affinity MCP server at ' + SERVER_URL + '...');

  const sseRes = await fetch(SERVER_URL);
  if (!sseRes.ok) {
    throw new Error('Failed to connect to SSE: HTTP ' + sseRes.status);
  }

  const reader = sseRes.body.getReader();
  const decoder = new TextDecoder();
  let endpointPath = null;
  let sseBuffer = '';

  const pendingRequests = new Map();
  let nextId = 1;

  function handleMessage(msgStr) {
    try {
      const data = JSON.parse(msgStr);
      if (data.id && pendingRequests.has(data.id)) {
        const { resolve, reject } = pendingRequests.get(data.id);
        pendingRequests.delete(data.id);
        if (data.error) {
          reject(new Error(data.error.message || JSON.stringify(data.error)));
        } else {
          resolve(data.result);
        }
      }
    } catch (e) {
      console.warn('Failed to parse message:', e.message, msgStr);
    }
  }

  // Background SSE read loop
  (async () => {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      sseBuffer += decoder.decode(value, { stream: true });
      const lines = sseBuffer.split(/\r?\n/);
      sseBuffer = lines.pop(); // keep partial line

      let currentEvent = null;
      for (const line of lines) {
        if (line.startsWith('event:')) {
          currentEvent = line.substring(6).trim();
        } else if (line.startsWith('data:')) {
          const data = line.substring(5).trim();
          if (currentEvent === 'endpoint') {
            endpointPath = data;
          } else if (currentEvent === 'message') {
            handleMessage(data);
          }
        }
      }
    }
  })().catch(err => console.error('SSE loop error:', err.message));

  // Wait for endpoint
  while (!endpointPath) {
    await new Promise(r => setTimeout(r, 50));
  }

  const postUrl = new URL(endpointPath, SERVER_URL).toString();
  console.log('MCP Endpoint ready:', postUrl);

  async function rpcCall(method, params = {}) {
    const id = nextId++;
    const promise = new Promise((resolve, reject) => {
      pendingRequests.set(id, { resolve, reject });
      setTimeout(() => {
        if (pendingRequests.has(id)) {
          pendingRequests.delete(id);
          reject(new Error(`RPC timeout for ${method} (id: ${id})`));
        }
      }, 30000);
    });

    const res = await fetch(postUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params }),
    });

    if (!res.ok) {
      pendingRequests.delete(id);
      throw new Error(`HTTP POST error ${res.status} for ${method}`);
    }

    return promise;
  }

  async function notify(method, params = {}) {
    await fetch(postUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', method, params }),
    });
  }

  // Initialize
  console.log('Initializing MCP protocol...');
  await rpcCall('initialize', {
    protocolVersion: '2025-11-25',
    capabilities: {},
    clientInfo: { name: 'affinity-doc-exporter', version: '1.0.0' },
  });
  await notify('notifications/initialized');

  // Prime preamble
  try {
    await rpcCall('tools/call', {
      name: 'read_sdk_documentation_topic',
      arguments: { filename: 'preamble' },
    });
  } catch (e) {
    // best-effort
  }

  // Get topic list
  console.log('Fetching SDK documentation topic list...');
  const listResult = await rpcCall('tools/call', {
    name: 'list_sdk_documentation',
    arguments: {},
  });

  const listText = (listResult?.content?.[0]?.text || '').trim();
  const fileNames = listText
    .split(/[,\r\n]+/)
    .map(s => s.trim())
    .filter(n => n && !/^error/i.test(n) && n !== 'preamble');

  console.log(`Found ${fileNames.length} topics:`, fileNames);

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  let savedCount = 0;
  for (const fileName of fileNames) {
    const outName = fileName.endsWith('.md') ? fileName : `${fileName}.md`;
    const outPath = path.join(OUTPUT_DIR, outName);
    if (fs.existsSync(outPath)) {
      continue;
    }
    try {
      const readResult = await rpcCall('tools/call', {
        name: 'read_sdk_documentation_topic',
        arguments: { filename: fileName },
      });
      const content = readResult?.content?.[0]?.text;
      if (content && !/^error[:\s]/i.test(content.trim())) {
        fs.mkdirSync(path.dirname(outPath), { recursive: true });
        fs.writeFileSync(outPath, content, 'utf8');
        console.log(`Saved: docs/${outName}`);
        savedCount++;
      } else {
        console.warn(`Skipped ${fileName}: empty or error response`);
      }
    } catch (e) {
      console.warn(`Failed to fetch ${fileName}:`, e.message);
    }
  }

  console.log(`\nSuccessfully saved ${savedCount} Markdown documents to: ${OUTPUT_DIR}`);
  process.exit(0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
