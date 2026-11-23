import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import * as fs from 'node:fs';
import { request as httpRequest } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { PassThrough } from 'node:stream';
import ssh2 from 'ssh2';
import { StagingStore } from '../build/staging/staging-store.js';

const { Server, utils } = ssh2;
const entrypoint = new URL('../build/index.js', import.meta.url);
const master = 'test-master-token';
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ssh-mcp-staging-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}

async function eventually(check) {
  const deadline = Date.now() + 8000;
  for (;;) {
    try {
      return check();
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await sleep(10);
    }
  }
}

function launch(t, dir, { port = 22, token = master, args = [], env = {}, mode = 'http' } = {}) {
  const config = path.join(dir, 'servers.json');
  fs.writeFileSync(config, JSON.stringify({
    default: {
      host: '127.0.0.1', port, username: 'test', password: 'test',
      allowedLocalPaths: [dir], allowedRemotePaths: ['/files'],
      sftpTimeoutMs: 5000,
    },
  }));
  const stagingDir = path.join(dir, 'staging');
  const child = spawn(process.execPath, [
    entrypoint.pathname, '--config-file', config,
    '--mcp-transport', mode, '--http-port', '0',
    '--staging-dir', stagingDir,
    ...(token ? ['--http-token', token] : []), ...args,
  ], {
    stdio: ['pipe', 'pipe', 'pipe'],
    env: {
      ...process.env,
      SSH_MCP_HTTP_TOKEN: '',
      SSH_MCP_HTTP_PUBLIC_URL: undefined,
      SSH_MCP_STAGING_MAX_FILE_BYTES: undefined,
      SSH_MCP_STAGING_MAX_TOTAL_BYTES: undefined,
      SSH_MCP_STAGING_TTL_MS: undefined,
      ...env,
    },
  });
  const exited = once(child, 'exit');
  t.after(async () => {
    child.stdin.end();
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
    await exited;
  });
  let logs = '';
  child.stderr.on('data', (chunk) => { logs += chunk; });
  const endpoint = async () => eventually(() => {
    const match = logs.match(/MCP HTTP server listening on (http:\/\/\S+)/);
    assert.ok(match, logs);
    return match[1].replace(/\/$/, '');
  });
  return { child, stagingDir, endpoint, exited, logs: () => logs };
}

function callHttp(url, { method = 'GET', token = master, body, headers = {}, end = true, requestPath } = {}) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = httpRequest(url, {
      method,
      ...(requestPath && { path: requestPath }),
      headers: { ...(token && { Authorization: `Bearer ${token}` }), ...headers },
    }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => {
        const bytes = Buffer.concat(chunks);
        resolve({ status: res.statusCode, headers: res.headers, bytes, json: () => JSON.parse(bytes) });
      });
    });
    req.on('error', reject);
    if (end) req.end(body);
    else if (body) req.write(body);
  });
  return { req, response };
}

function request(url, options) {
  return callHttp(url, options).response;
}

async function tool(endpoint, name, args, token = master) {
  const body = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } });
  const res = await request(endpoint, {
    method: 'POST', token, body,
    headers: {
      Accept: 'application/json, text/event-stream',
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  });
  assert.equal(res.status, 200, res.bytes.toString());
  const { result, error } = res.json();
  assert.ok(result, JSON.stringify(error));
  const content = result.content[0].text;
  return content.startsWith('{') ? JSON.parse(content) : content;
}

async function startSftp(t, dir) {
  const { privateKey } = generateKeyPairSync('rsa', {
    modulusLength: 2048,
    privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
    publicKeyEncoding: { type: 'pkcs1', format: 'pem' },
  });
  const root = path.join(dir, 'remote');
  fs.mkdirSync(root);
  const clients = new Set();
  const metrics = { reads: 0, writes: 0, maxReads: 0, maxWrites: 0 };
  const { STATUS_CODE, flagsToString } = utils.sftp;
  const complete = (sftp, id, error) => sftp.status(id, error ? STATUS_CODE.FAILURE : STATUS_CODE.OK);
  function mapPath(remote) {
    assert.match(remote, /^\/files\/[a-z0-9.-]+$/);
    return path.join(root, path.posix.basename(remote));
  }
  const ssh = new Server({ hostKeys: [privateKey] }, (client) => {
    clients.add(client);
    client.on('error', () => {});
    client.on('close', () => clients.delete(client));
    client.on('authentication', (ctx) => {
      if (ctx.method === 'password' && ctx.password === 'test' && ctx.username === 'test') ctx.accept();
      else ctx.reject(['password']);
    });
    client.on('ready', () => client.on('session', (accept) => {
      const session = accept();
      session.on('sftp', (acceptSftp) => {
        const sftp = acceptSftp();
        const openFiles = new Map();
        let nextHandle = 0;
        const get = (handle) => openFiles.get(handle.readUInt32BE());
        const stat = (id, error, data) => {
          if (error) sftp.status(id, error.code === 'ENOENT' ? STATUS_CODE.NO_SUCH_FILE : STATUS_CODE.FAILURE);
          else sftp.attrs(id, {
            mode: data.mode, size: data.size, uid: data.uid, gid: data.gid,
            atime: Math.floor(data.atimeMs / 1000), mtime: Math.floor(data.mtimeMs / 1000),
          });
        };
        sftp.on('STAT', (id, remote) => fs.stat(mapPath(remote), (err, data) => stat(id, err, data)));
        sftp.on('FSTAT', (id, handle) => fs.fstat(get(handle), (err, data) => stat(id, err, data)));
        sftp.on('OPEN', (id, remote, flags) => {
          fs.open(mapPath(remote), flagsToString(flags), 0o600, (err, fd) => {
            if (err) return complete(sftp, id, err);
            const key = nextHandle++;
            openFiles.set(key, fd);
            const handle = Buffer.alloc(4);
            handle.writeUInt32BE(key);
            sftp.handle(id, handle);
          });
        });
        sftp.on('READ', (id, handle, offset, length) => {
          const data = Buffer.alloc(length);
          metrics.maxReads = Math.max(metrics.maxReads, ++metrics.reads);
          fs.read(get(handle), data, 0, length, offset, (err, count) => {
            metrics.reads--;
            if (err) complete(sftp, id, err);
            else if (count === 0) sftp.status(id, STATUS_CODE.EOF);
            else sftp.data(id, data.subarray(0, count));
          });
        });
        sftp.on('WRITE', (id, handle, offset, bytes) => {
          metrics.maxWrites = Math.max(metrics.maxWrites, ++metrics.writes);
          fs.write(get(handle), bytes, 0, bytes.length, offset, (err) => {
            metrics.writes--;
            complete(sftp, id, err);
          });
        });
        sftp.on('CLOSE', (id, handle) => {
          const fd = get(handle);
          openFiles.delete(handle.readUInt32BE());
          fs.close(fd, (err) => complete(sftp, id, err));
        });
        sftp.on('close', () => {
          for (const fd of openFiles.values()) fs.close(fd, () => {});
          openFiles.clear();
        });
      });
    }));
  });
  await new Promise((resolve, reject) => {
    ssh.once('error', reject);
    ssh.listen(0, '127.0.0.1', resolve);
  });
  t.after(async () => {
    for (const client of clients) client.end();
    await new Promise((resolve) => ssh.close(resolve));
  });
  return { port: ssh.address().port, root, metrics };
}

describe('HTTP staging file transfers', () => {
  it('authenticates master requests and atomically redeems scoped tickets', async (t) => {
    const server = launch(t, fixture(t));
    const endpoint = await server.endpoint();
    const files = endpoint + '/files';
    for (const token of [null, 'wrong']) {
      const res = await request(files, { method: 'POST', token, body: 'anonymous' });
      assert.equal(res.status, 401);
      assert.equal(res.headers['www-authenticate'], 'Bearer realm="ssh-mcp-server"');
    }
    assert.deepEqual(fs.readdirSync(server.stagingDir), []);

    const a = await tool(endpoint, 'prepare-upload', { fileName: '../../etc/passwd', expectedSizeBytes: 3 });
    const b = await tool(endpoint, 'prepare-upload', {});
    assert.equal(a.uploadUrl, files + '/' + a.transferId);
    assert.equal(a.method, 'PUT');
    assert.equal((await request(a.uploadUrl, { token: a.transferToken })).status, 401);
    assert.equal((await request(b.uploadUrl, { method: 'PUT', token: a.transferToken, body: 'abc' })).status, 401);
    const upload = { method: 'PUT', token: a.transferToken, body: 'abc' };
    const replies = await Promise.all([request(a.uploadUrl, upload), request(a.uploadUrl, upload)]);
    assert.deepEqual(replies.map((res) => res.status).sort(), [201, 401]);
    const staged = replies.find((res) => res.status === 201).json();
    assert.equal(staged.fileName, 'passwd');
    assert.equal(staged.sha256, digest('abc'));
    assert.deepEqual(fs.readdirSync(server.stagingDir), [a.transferId + '.bin']);
    assert.equal(fs.statSync(path.join(server.stagingDir, a.transferId + '.bin')).mode & 0o777, 0o600);

    const got = await request(a.uploadUrl);
    assert.equal(got.status, 200);
    assert.equal(got.headers['x-content-sha256'], staged.sha256);
    assert.equal(got.headers['cache-control'], 'no-store');
    assert.match(got.headers['content-disposition'], /filename="passwd"/);
    assert.equal(got.bytes.toString(), 'abc');
    assert.equal((await request(files)).status, 405);
    const direct = await request(files, { method: 'POST', body: 'direct', headers: { 'X-File-Name': '..\\quoted".bin' } });
    assert.equal(direct.status, 201);
    const directFile = direct.json();
    assert.equal(directFile.fileName, 'quoted".bin');
    const directUrl = files + '/' + directFile.transferId;
    assert.equal((await request(directUrl)).bytes.toString(), 'direct');
    const deleted = (await request(directUrl, { method: 'DELETE' })).json();
    assert.equal(deleted.transferId, directFile.transferId);
    assert.equal(deleted.deleted, true);
    assert.ok([0, 6].includes(deleted.freedBytes));
    assert.equal((await request(directUrl)).status, 404);
    await eventually(() => assert.equal(fs.existsSync(path.join(server.stagingDir, directFile.transferId + '.bin')), false));
    assert.equal((await tool(endpoint, 'cleanup-transfer', { transferId: a.transferId })).freedBytes, 3);
  });

  it('rejects expired tickets and requires a ticket when there is no master token', async (t) => {
    const server = launch(t, fixture(t), { token: null, args: ['--staging-ttl-ms', '150'] });
    const endpoint = await server.endpoint();
    const options = { method: 'POST', token: null, body: 'a' };
    assert.equal((await request(endpoint + '/files', options)).status, 401);
    const fresh = await tool(endpoint, 'prepare-upload', {}, null);
    assert.equal((await request(fresh.uploadUrl, { ...options, token: fresh.transferToken })).status, 201);
    const expired = await tool(endpoint, 'prepare-upload', {}, null);
    await sleep(Math.max(0, expired.ticketExpiresAt - Date.now()) + 10);
    assert.equal((await request(expired.uploadUrl, { ...options, token: expired.transferToken })).status, 401);
    await eventually(() => assert.deepEqual(fs.readdirSync(server.stagingDir), []));
  });

  it('rejects invalid paths, hostile Host headers, and symlink files', async (t) => {
    const dir = fixture(t);
    const server = launch(t, dir);
    const endpoint = await server.endpoint();
    const files = endpoint + '/files';
    for (const suffix of ['/not-a-uuid', '/%2e%2e%2f' + randomUUID(), '/a/%2E%2E/' + randomUUID()]) {
      const requestPath = new URL(files).pathname + suffix;
      assert.equal((await request(files, { method: 'PUT', body: 'a', requestPath })).status, 400);
    }
    const hostile = await request(files, { method: 'POST', body: 'a', headers: { Host: 'example.test' } });
    assert.equal(hostile.status, 403);
    const { transferId } = (await request(files, { method: 'POST', body: 'abc' })).json();
    const outside = path.join(dir, 'external.txt');
    fs.writeFileSync(outside, 'outside');
    const staged = path.join(server.stagingDir, transferId + '.bin');
    fs.unlinkSync(staged);
    fs.symlinkSync(outside, staged);
    assert.equal((await request(files + '/' + transferId)).status, 400);
    assert.equal((await request(files + '/' + transferId, { method: 'DELETE' })).status, 400);
    assert.equal(fs.readFileSync(outside, 'utf8'), 'outside');
    for (const name of ['upload', 'download']) {
      const result = await tool(endpoint, name, { localPath: staged, remotePath: '/files/test.bin' });
      assert.equal(result.code, 'LOCAL_PATH_NOT_ALLOWED');
    }
  });

  it('rejects a symlink staging root at startup', async (t) => {
    const dir = fixture(t);
    fs.mkdirSync(path.join(dir, 'elsewhere'));
    fs.symlinkSync(path.join(dir, 'elsewhere'), path.join(dir, 'staging'));
    const server = launch(t, dir);
    const [code] = await server.exited;
    assert.notEqual(code, 0);
    assert.match(server.logs(), /Staging directory must be a real directory/);
  });

  it('caps declared and chunked upload sizes and avoids sending 100 Continue on rejection', async (t) => {
    const server = launch(t, fixture(t), { args: ['--staging-max-file-bytes', '8'] });
    const files = (await server.endpoint()) + '/files';
    const early = callHttp(files, {
      method: 'POST', end: false, headers: { 'Content-Length': '9', Expect: '100-continue' },
    });
    let continued = false;
    early.req.on('continue', () => { continued = true; });
    early.req.flushHeaders();
    assert.equal((await early.response).status, 413);
    assert.equal(continued, false);
    const chunked = callHttp(files, { method: 'POST', end: false });
    chunked.req.write('1234');
    chunked.req.end('56789');
    const rejected = await chunked.response;
    assert.equal(rejected.status, 413);
    assert.equal(rejected.json().code, 'FILE_TOO_LARGE');
    assert.deepEqual(fs.readdirSync(server.stagingDir), []);
    assert.equal((await request(files, { method: 'POST', body: '12345678' })).status, 201);
  });

  it('accounts for concurrent reservations and frees them after truncated or quota-rejected streams', async (t) => {
    const server = launch(t, fixture(t), {
      args: ['--staging-max-file-bytes', '12', '--staging-max-total-bytes', '12'],
    });
    const files = (await server.endpoint()) + '/files';
    const pending = callHttp(files, {
      method: 'POST', body: 'abc', end: false, headers: { 'Content-Length': '10' },
    });
    pending.response.catch(() => {});
    await eventually(() => assert.match(fs.readdirSync(server.stagingDir)[0] ?? '', /\.part-/));
    const rejected = await request(files, { method: 'POST', body: 'abc', headers: { 'Content-Length': '3' } });
    assert.equal(rejected.status, 507);
    assert.equal(rejected.json().code, 'STAGING_QUOTA_EXCEEDED');
    pending.req.destroy();
    await assert.rejects(pending.response);
    await eventually(() => assert.deepEqual(fs.readdirSync(server.stagingDir), []));

    assert.equal((await request(files, { method: 'POST', body: '123456789' })).status, 201);
    const chunked = callHttp(files, { method: 'POST', end: false });
    chunked.req.write('ab');
    chunked.req.end('cd');
    assert.equal((await chunked.response).status, 507);
    assert.equal((await request(files, { method: 'POST', body: 'abc' })).status, 201);
    assert.equal(fs.readdirSync(server.stagingDir).filter((name) => name.includes('.part-')).length, 0);
  });

  it('restores live entries and cleans orphan parts and expired files on startup and on the timer', async (t) => {
    const dir = fixture(t);
    const stagingDir = path.join(dir, 'staging');
    fs.mkdirSync(stagingDir, 0o700);
    const live = randomUUID();
    const expired = randomUUID();
    fs.writeFileSync(path.join(stagingDir, '.part-orphan'), 'abc');
    fs.writeFileSync(path.join(stagingDir, live + '.bin'), 'restored');
    const expiredPath = path.join(stagingDir, expired + '.bin');
    fs.writeFileSync(expiredPath, 'old');
    const then = (Date.now() - 20000) / 1000;
    fs.utimesSync(expiredPath, then, then);
    const store = new StagingStore({ dir: stagingDir, maxTotalBytes: 10, ttlMs: 200 });
    t.after(() => store.close());
    assert.deepEqual(fs.readdirSync(stagingDir), [live + '.bin']);
    assert.throws(() => store.prepareUpload('overflow', 3), { code: 'STAGING_QUOTA_EXCEEDED' });
    const sha256 = await store.withFile(live, async (file) => file.sha256);
    assert.equal(sha256, digest('restored'));
    await eventually(() => assert.deepEqual(fs.readdirSync(stagingDir), []));
    assert.ok(store.prepareUpload('fits', 10).transferToken);
  });

  it('defers deletion and quota release until the final reader exits', async (t) => {
    const store = new StagingStore({ dir: fixture(t), maxTotalBytes: 3 });
    t.after(() => store.close());
    const data = new PassThrough();
    const staged = store.saveNew('three', 3, { stream: () => data });
    data.end('abc');
    const { transferId } = await staged;
    let release;
    let ready;
    const started = new Promise((resolve) => { ready = resolve; });
    const held = new Promise((resolve) => { release = resolve; });
    const reading = store.withFile(transferId, async () => { ready(); await held; });
    await started;
    assert.equal(store.cleanup(transferId).freedBytes, 0);
    assert.throws(() => store.prepareUpload('one', 1), { code: 'STAGING_QUOTA_EXCEEDED' });
    await assert.rejects(store.withFile(transferId, async () => {}), { code: 'TRANSFER_NOT_FOUND' });
    release();
    await reading;
    assert.deepEqual(fs.readdirSync(store.root), []);
    assert.ok(store.prepareUpload('three', 3).transferToken);
  });

  it('preserves a staged file when a download disconnects before it finishes', async (t) => {
    const server = launch(t, fixture(t));
    const files = (await server.endpoint()) + '/files';
    const body = randomBytes(8 * 1024 * 1024);
    const staged = (await request(files, { method: 'POST', body, headers: { 'Content-Length': body.length } })).json();
    const url = files + '/' + staged.transferId;
    await new Promise((resolve, reject) => {
      const req = httpRequest(url + '?consume=1', { headers: { Authorization: `Bearer ${master}` } }, (res) => {
        res.once('data', () => {
          assert.equal(res.statusCode, 200);
          res.destroy();
          resolve();
        });
        res.once('error', reject);
      });
      req.once('error', reject);
      req.end();
    });
    const retry = await request(url + '?consume=true');
    assert.equal(retry.status, 200);
    assert.equal(digest(retry.bytes), staged.sha256);
    await eventually(() => assert.deepEqual(fs.readdirSync(server.stagingDir), []));
  });

  it('supports public URL and environment limits with CLI precedence', async (t) => {
    const dir = fixture(t);
    const envDir = path.join(dir, 'env-staging');
    const server = launch(t, dir, {
      env: {
        SSH_MCP_HTTP_PUBLIC_URL: 'https://files.example.test/ssh-mcp///',
        SSH_MCP_STAGING_DIR: envDir,
        SSH_MCP_STAGING_MAX_FILE_BYTES: '2',
        SSH_MCP_STAGING_MAX_TOTAL_BYTES: '5',
      },
      args: ['--staging-max-file-bytes', '4', '--http-path', '/'],
    });
    const endpoint = await server.endpoint();
    const staged = await tool(endpoint, 'prepare-upload', { expectedSizeBytes: 4 });
    assert.equal(staged.uploadUrl, 'https://files.example.test/ssh-mcp/files/' + staged.transferId);
    assert.equal(staged.maxFileBytes, 4);
    assert.equal(fs.existsSync(envDir), false);
    const direct = await request(endpoint + '/files', {
      method: 'POST', body: '1234', headers: { 'Content-Length': '4' },
    });
    assert.equal(direct.status, 201);
    assert.equal((await tool(endpoint, 'prepare-upload', { expectedSizeBytes: 2 })).code, 'STAGING_QUOTA_EXCEEDED');
  });

  it('requires HTTP for staging tools without creating a staging directory in stdio', async (t) => {
    const server = launch(t, fixture(t), { mode: 'stdio', env: { SSH_MCP_STAGING_TTL_MS: 'invalid-ignored' } });
    let output = '';
    server.child.stdout.on('data', (chunk) => { output += chunk; });
    server.child.stdin.write(JSON.stringify({
      jsonrpc: '2.0', id: 1, method: 'tools/call',
      params: { name: 'prepare-upload', arguments: {} },
    }) + '\n');
    await eventually(() => assert.match(output, /HTTP_TRANSPORT_REQUIRED/));
    assert.equal(fs.existsSync(server.stagingDir), false);
  });

  it('round-trips 512 KiB through real concurrent SFTP, verifies hashes and honors consume controls', { timeout: 20000 }, async (t) => {
    const dir = fixture(t);
    const remote = await startSftp(t, dir);
    const server = launch(t, dir, { port: remote.port });
    const endpoint = await server.endpoint();
    const bytes = randomBytes(512 * 1024);
    const sha256 = digest(bytes);
    const upload = await tool(endpoint, 'prepare-upload', { fileName: 'large.bin', expectedSizeBytes: bytes.length });

    const received = await request(upload.uploadUrl, {
      method: 'PUT', token: upload.transferToken, body: bytes, headers: { 'Content-Length': bytes.length },
    });
    assert.equal(received.status, 201, received.bytes.toString());
    assert.equal(received.json().sha256, sha256);
    const args = { transferId: upload.transferId, remotePath: '/files/large.bin', verifySha256: true };
    assert.equal((await tool(endpoint, 'upload', { remotePath: args.remotePath })).code, 'INVALID_TRANSFER_ARGUMENTS');
    assert.equal((await tool(endpoint, 'upload', { ...args, localPath: dir })).code, 'INVALID_TRANSFER_ARGUMENTS');
    const uploaded = await tool(endpoint, 'upload', args);
    assert.deepEqual(uploaded, {
      transferId: upload.transferId, remotePath: args.remotePath,
      sizeBytes: bytes.length, sha256, verifiedSha256: true, cleanedUp: true,
    });
    assert.ok(remote.metrics.maxWrites > 1, 'fastPut overlaps writes');
    assert.equal(digest(fs.readFileSync(path.join(remote.root, 'large.bin'))), sha256);
    assert.deepEqual(fs.readdirSync(server.stagingDir), []);

    remote.metrics.maxReads = 0;
    const staged = await tool(endpoint, 'download', { remotePath: args.remotePath });
    assert.equal(staged.sha256, sha256);
    assert.equal(staged.sizeBytes, bytes.length);
    assert.ok(remote.metrics.maxReads > 1, 'fastGet overlaps reads');
    assert.equal((await request(staged.downloadUrl, { method: 'DELETE', token: staged.transferToken })).status, 401);
    const pulled = await request(staged.downloadUrl, { token: staged.transferToken });
    assert.equal(pulled.status, 200);
    assert.equal(pulled.headers['x-content-sha256'], sha256);
    assert.deepEqual(pulled.bytes, bytes);
    assert.equal((await request(staged.downloadUrl, { token: staged.transferToken })).status, 401);
    await eventually(() => assert.deepEqual(fs.readdirSync(server.stagingDir), []));

    const legacySource = path.join(dir, 'legacy.bin');
    const legacyDestination = path.join(dir, 'legacy-down.bin');
    fs.writeFileSync(legacySource, 'legacy');
    assert.equal(await tool(endpoint, 'upload', { localPath: legacySource, remotePath: '/files/legacy.bin' }), 'File uploaded successfully');
    assert.equal(await tool(endpoint, 'download', { localPath: legacyDestination, remotePath: '/files/legacy.bin' }), 'File downloaded successfully');
    assert.equal(fs.readFileSync(legacyDestination, 'utf8'), 'legacy');

    const retained = await tool(endpoint, 'download', { remotePath: '/files/legacy.bin' });
    assert.equal((await request(retained.downloadUrl + '?consume=0', { token: retained.transferToken })).bytes.toString(), 'legacy');
    assert.equal((await request(retained.downloadUrl)).bytes.toString(), 'legacy');
    const retainedByTool = await tool(endpoint, 'download', { remotePath: '/files/legacy.bin', consumeOnDownload: false });
    assert.equal((await request(retainedByTool.downloadUrl, { token: retainedByTool.transferToken })).bytes.toString(), 'legacy');
    assert.equal((await request(retainedByTool.downloadUrl + '?consume=true')).bytes.toString(), 'legacy');
    await eventually(() => assert.equal(fs.readdirSync(server.stagingDir).length, 1));
  });
});
