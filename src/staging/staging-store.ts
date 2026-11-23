import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { type Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Logger } from "../utils/logger.js";
import { isPathWithinRoot } from "../utils/local-path.js";
import { ToolError, type ToolErrorCode } from "../utils/tool-error.js";

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const FIVE_MINUTES = 5 * 60 * 1000;

export interface StagingConfig {
  dir?: string;
  maxFileBytes?: number;
  maxTotalBytes?: number;
  ttlMs?: number;
}

interface FileMetadata {
  transferId: string;
  fileName: string;
  sizeBytes: number;
  sha256: string;
  expiresAt: number;
}

interface Entry extends Omit<FileMetadata, "sha256"> {
  sha256?: string;
  stats: fs.Stats;
  activeReaders: number;
  pendingDelete: boolean;
}

interface Ticket {
  token: Buffer;
  purpose: "upload" | "download";
  expiresAt: number;
  fileName?: string;
  consume?: boolean;
}

export type StagingSource =
  | {
      stream: () => Readable;
      signal?: AbortSignal;
      start?: () => void;
      assertComplete?: () => void;
    }
  | { fastGet: (partPath: string) => Promise<void> };

export class StagingError extends ToolError {
  constructor(code: ToolErrorCode, message: string, public readonly status: number) {
    super(code, message);
  }
}

export function normalizeTransferId(value: string): string {
  if (!UUID_V4.test(value)) {
    throw new StagingError("INVALID_TRANSFER_ID", "Expected a UUID v4 transferId.", 400);
  }
  return value.toLowerCase();
}

function stagingDirectory(): string {
  const parent = "/var/lib/ssh-mcp";
  try {
    if (fs.statSync(parent).isDirectory()) {
      fs.accessSync(parent, fs.constants.W_OK | fs.constants.X_OK);
      return path.join(parent, "_staging");
    }
  } catch {
    // Local users typically lack access to the service directory.
  }
  return path.join(os.tmpdir(), "ssh-mcp-staging-" + (process.getuid?.() ?? process.pid));
}

function cleanFileName(name: string | undefined, id: string): string {
  const base = path.posix.basename((name ?? "").replace(/\\/g, "/"));
  const cleaned = base.replace(/[\p{Cc}\p{Cf}/\\]/gu, "").trim();
  return cleaned && cleaned !== "." && cleaned !== ".."
    ? Buffer.from(Array.from(cleaned).slice(0, 255).join("")).toString("utf8")
    : `${id}.bin`;
}

function hashStream(source: Readable): Promise<string> {
  const hash = createHash("sha256");
  return pipeline(source, hash).then(() => hash.digest("hex"));
}

export class StagingStore {
  readonly root: string;
  readonly maxFileBytes: number;
  readonly maxTotalBytes: number;
  readonly ttlMs: number;
  private readonly rootStats: fs.Stats;
  private readonly entries = new Map<string, Entry>();
  private readonly tickets = new Map<string, Ticket>();
  private readonly writing = new Set<string>();
  private readonly timer: NodeJS.Timeout;
  private storedBytes = 0;
  private reservedBytes = 0;

  constructor(config: StagingConfig = {}) {
    this.maxFileBytes = config.maxFileBytes ?? 104857600;
    this.maxTotalBytes = config.maxTotalBytes ?? 1073741824;
    this.ttlMs = config.ttlMs ?? 1800000;

    const dir = path.resolve(config.dir ?? stagingDirectory());
    if (!fs.lstatSync(dir, { throwIfNoEntry: false })) {
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    }
    const stats = fs.lstatSync(dir);
    if (!stats.isDirectory() || stats.isSymbolicLink() || (stats.mode & 0o022) !== 0 ||
        (process.getuid && stats.uid !== process.getuid())) {
      throw new Error("Staging directory must be a real directory owned by the service user and protected from other writers.");
    }
    this.root = fs.realpathSync.native(dir);
    this.rootStats = stats;
    this.restore();
    this.timer = setInterval(() => {
      try {
        this.sweepExpired();
      } catch (error) {
        Logger.log(`Staging cleanup failed: ${(error as Error).message}`, "error");
      }
    }, Math.min(this.ttlMs, FIVE_MINUTES)).unref();
  }

  close(): void {
    clearInterval(this.timer);
  }

  private assertRoot(): void {
    const stats = fs.lstatSync(this.root);
    if (!stats.isDirectory() || stats.dev !== this.rootStats.dev || stats.ino !== this.rootStats.ino ||
        fs.realpathSync.native(this.root) !== this.root) {
      throw new StagingError("STAGING_PATH_INVALID", "Staging directory has changed.", 500);
    }
  }

  private checkedFile(filePath: string): fs.Stats {
    this.assertRoot();
    const stats = fs.lstatSync(filePath, { throwIfNoEntry: false });
    if (!stats) {
      throw new StagingError("TRANSFER_NOT_FOUND", "Staged file does not exist.", 404);
    }
    if (!stats.isFile() || stats.isSymbolicLink() || stats.nlink !== 1 ||
        !isPathWithinRoot(fs.realpathSync.native(filePath), this.root)) {
      throw new StagingError("STAGING_PATH_INVALID", "Unsafe staged file.", 400);
    }
    return stats;
  }

  private restore(): void {
    const now = Date.now();
    for (const name of fs.readdirSync(this.root)) {
      const filePath = path.join(this.root, name);
      if (/\.part-.+$/.test(name) || name.startsWith(".part-")) {
        fs.unlinkSync(filePath);
        continue;
      }
      const id = name.endsWith(".bin") ? name.slice(0, -4) : "";
      if (!UUID_V4.test(id) || id !== id.toLowerCase()) {
        throw new Error(`Unexpected entry in dedicated staging directory: ${name}`);
      }
      const stats = this.checkedFile(filePath);
      const expiresAt = stats.mtimeMs + this.ttlMs;
      if (expiresAt <= now) {
        fs.unlinkSync(filePath);
        continue;
      }
      this.entries.set(id, {
        transferId: id, fileName: name, sizeBytes: stats.size, expiresAt,
        stats, activeReaders: 0, pendingDelete: false,
      });
      this.storedBytes += stats.size;
    }
    this.checkCapacity(0, 0);
  }

  private checkCapacity(fileBytes: number, additionalBytes: number): void {
    if (fileBytes > this.maxFileBytes) {
      throw new StagingError("FILE_TOO_LARGE", "Transfer exceeds the staging per-file limit.", 413);
    }
    if (additionalBytes > this.maxTotalBytes - this.storedBytes - this.reservedBytes) {
      this.sweepExpired();
      if (additionalBytes > this.maxTotalBytes - this.storedBytes - this.reservedBytes) {
        throw new StagingError("STAGING_QUOTA_EXCEEDED", "Staging capacity is exhausted.", 507);
      }
    }
  }

  prepareUpload(fileName?: string, expectedSizeBytes?: number) {
    this.sweepExpired();
    this.checkCapacity(expectedSizeBytes ?? 0, expectedSizeBytes ?? 0);
    const transferId = randomUUID();
    return {
      transferId,
      ...this.issueTicket(transferId, { purpose: "upload", fileName: cleanFileName(fileName, transferId) }),
    };
  }

  issueDownload(transferId: string, consume: boolean) {
    return this.issueTicket(transferId, { purpose: "download", consume });
  }

  private issueTicket(id: string, options: Omit<Ticket, "token" | "expiresAt">) {
    const transferToken = randomBytes(32).toString("base64url");
    const ticketExpiresAt = Date.now() + Math.min(this.ttlMs, FIVE_MINUTES);
    this.tickets.set(id, { ...options, token: Buffer.from(transferToken), expiresAt: ticketExpiresAt });
    return { transferToken, ticketExpiresAt };
  }

  redeemTicket(id: string, purpose: Ticket["purpose"], bearer: string): Ticket | undefined {
    const ticket = this.tickets.get(id);
    if (!ticket || ticket.purpose !== purpose || ticket.expiresAt <= Date.now()) {
      return undefined;
    }
    const provided = Buffer.from(bearer);
    if (provided.length !== ticket.token.length || !timingSafeEqual(provided, ticket.token)) {
      return undefined;
    }
    this.tickets.delete(id);
    return ticket;
  }

  preparedFileName(id: string): string | undefined {
    const ticket = this.tickets.get(id);
    return ticket?.purpose === "upload" && ticket.expiresAt > Date.now() ? ticket.fileName : undefined;
  }

  saveNew(fileName: string, size: number, source: StagingSource): Promise<FileMetadata> {
    return this.save(randomUUID(), fileName, size, source);
  }

  async save(
    transferId: string,
    fileName: string | undefined,
    expectedSize: number | undefined,
    source: StagingSource,
  ): Promise<FileMetadata> {
    const id = normalizeTransferId(transferId);
    this.sweepExpired();
    this.assertRoot();
    const binPath = path.join(this.root, `${id}.bin`);
    if (this.entries.has(id) || this.writing.has(id) || fs.lstatSync(binPath, { throwIfNoEntry: false })) {
      throw new StagingError("TRANSFER_CONFLICT", "A file already exists or is in progress for this transferId.", 409);
    }

    let reserved = expectedSize ?? 0;
    this.checkCapacity(reserved, reserved);
    this.reservedBytes += reserved;
    this.writing.add(id);
    const partPath = path.join(this.root, `${id}.part-${randomBytes(16).toString("hex")}`);
    let ownedPart = false;
    let sizeBytes = 0;
    let sha256: string;
    try {
      const fd = fs.openSync(partPath, "wx", 0o600);
      ownedPart = true;
      if ("fastGet" in source) {
        fs.closeSync(fd);
        await source.fastGet(partPath);
        // fastGet writes at offsets and is capped at the reserved size.
        const stats = this.checkedFile(partPath);
        sizeBytes = stats.size;
        if (sizeBytes !== expectedSize) {
          throw new StagingError("TRANSFER_INCOMPLETE", "Remote file size differs from its initial stat.", 400);
        }
        const handle = await fs.promises.open(partPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
        sha256 = await hashStream(handle.createReadStream());
      } else {
        const hash = createHash("sha256");
        const meter = new Transform({
          transform: (chunk: Buffer, _encoding, callback) => {
            const nextSize = sizeBytes + chunk.length;
            try {
              if (expectedSize !== undefined && nextSize > expectedSize) {
                throw new StagingError("TRANSFER_INCOMPLETE", "Received more bytes than expected.", 400);
              }
              const extra = Math.max(nextSize - reserved, 0);
              this.checkCapacity(nextSize, extra);
              this.reservedBytes += extra;
              reserved += extra;
              sizeBytes = nextSize;
              hash.update(chunk);
              callback(null, chunk);
            } catch (error) {
              callback(error as Error);
            }
          },
        });
        let input: Readable;
        try {
          input = source.stream();
        } catch (error) {
          fs.closeSync(fd);
          throw error;
        }
        const writing = pipeline(input, meter, fs.createWriteStream(partPath, { fd }), { signal: source.signal });
        source.start?.();
        await writing;
        source.assertComplete?.();
        if (expectedSize !== undefined && sizeBytes !== expectedSize) {
          throw new StagingError("TRANSFER_INCOMPLETE", "Received fewer bytes than expected.", 400);
        }
        sha256 = hash.digest("hex");
      }
      this.checkedFile(partPath);
      const now = Date.now();
      fs.utimesSync(partPath, now / 1000, now / 1000);
      const stats = this.checkedFile(partPath);
      fs.renameSync(partPath, binPath);
      ownedPart = false;
      const result = {
        transferId: id, fileName: cleanFileName(fileName, id), sizeBytes, sha256,
        expiresAt: now + this.ttlMs,
      };
      this.entries.set(id, { ...result, stats, activeReaders: 0, pendingDelete: false });
      this.tickets.delete(id);
      this.reservedBytes -= reserved;
      reserved = 0;
      this.storedBytes += sizeBytes;
      return result;
    } finally {
      try {
        if (ownedPart) {
          this.assertRoot();
          fs.unlinkSync(partPath);
        }
      } finally {
        this.writing.delete(id);
        this.reservedBytes -= reserved;
      }
    }
  }

  async withFile<T>(
    transferId: string,
    read: (file: FileMetadata, filePath: string, stream: () => Readable) => Promise<T>,
  ): Promise<T> {
    const id = normalizeTransferId(transferId);
    this.sweepExpired();
    const entry = this.entries.get(id);
    if (!entry || entry.pendingDelete) {
      throw new StagingError("TRANSFER_NOT_FOUND", "Staged file does not exist or has expired.", 404);
    }
    const filePath = path.join(this.root, `${id}.bin`);
    this.checkEntry(entry, this.checkedFile(filePath));
    entry.activeReaders++;
    let handle: fs.promises.FileHandle | undefined;
    try {
      handle = await fs.promises.open(filePath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
      this.checkEntry(entry, await handle.stat());
      const openedFile = handle;
      const stream = () => openedFile.createReadStream({ start: 0, autoClose: false });
      const sha256 = entry.sha256 ??= await hashStream(stream());
      const { transferId, fileName, sizeBytes, expiresAt } = entry;
      return await read({ transferId, fileName, sizeBytes, sha256, expiresAt }, filePath, stream);
    } finally {
      try {
        await handle?.close();
      } finally {
        entry.activeReaders--;
        if (entry.pendingDelete && entry.activeReaders === 0) {
          this.cleanup(id);
        }
      }
    }
  }

  private checkEntry(entry: Entry, stats: fs.Stats): void {
    if (!stats.isFile() || stats.nlink !== 1 || stats.ino !== entry.stats.ino ||
        stats.dev !== entry.stats.dev || stats.size !== entry.sizeBytes ||
        stats.mtimeMs !== entry.stats.mtimeMs) {
      throw new StagingError("STAGING_PATH_INVALID", "Staged file has changed.", 400);
    }
  }

  cleanup(transferId: string) {
    const id = normalizeTransferId(transferId);
    const revoked = this.tickets.delete(id);
    const entry = this.entries.get(id);
    if (!entry) {
      return { transferId: id, deleted: revoked, freedBytes: 0 };
    }
    entry.pendingDelete = true;
    if (entry.activeReaders > 0) {
      return { transferId: id, deleted: true, freedBytes: 0 };
    }
    const filePath = path.join(this.root, `${id}.bin`);
    try {
      this.checkEntry(entry, this.checkedFile(filePath));
      fs.unlinkSync(filePath);
    } catch (error) {
      if (!(error instanceof StagingError && error.code === "TRANSFER_NOT_FOUND")) {
        throw error;
      }
    }
    this.entries.delete(id);
    this.storedBytes -= entry.sizeBytes;
    return { transferId: id, deleted: true, freedBytes: entry.sizeBytes };
  }

  private sweepExpired(): void {
    const now = Date.now();
    for (const [id, ticket] of this.tickets) {
      if (ticket.expiresAt <= now) {
        this.tickets.delete(id);
      }
    }
    for (const [id, entry] of this.entries) {
      if (entry.expiresAt <= now) {
        try {
          this.cleanup(id);
        } catch (error) {
          Logger.log(`Cannot remove expired transfer ${id}: ${(error as Error).message}`, "error");
        }
      }
    }
  }
}
