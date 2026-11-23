import { randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { PassThrough } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Logger } from "../utils/logger.js";
import { normalizeTransferId, StagingError, StagingStore } from "./staging-store.js";

const UPLOAD_TIMEOUT_MS = 300000;

function json(res: ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(payload));
}

function contentDisposition(fileName: string): string {
  const fallback = fileName.replace(/[^\x20-\x7e]|["\\%]/g, "_");
  const encoded = encodeURIComponent(fileName).replace(/['()*]/g, (char) =>
    "%" + char.charCodeAt(0).toString(16).toUpperCase());
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encoded}`;
}

export class StagingHttpHandler {
  readonly path: string;

  constructor(private readonly store: StagingStore, basePath: string) {
    this.path = `${basePath === "/" ? "" : basePath}/files`;
  }

  async handle(
    req: IncomingMessage,
    res: ServerResponse,
    rawPath: string,
    bearer: string | undefined,
    masterAuthorized: boolean,
    allowedHosts?: string[],
    expectContinue = false,
  ): Promise<void> {
    const method = req.method;
    const uploading = method === "POST" || method === "PUT";
    try {
      if (allowedHosts && !allowedHosts.includes(req.headers.host ?? "")) {
        throw new StagingError("STAGING_PATH_INVALID", "Invalid Host header.", 403);
      }
      const suffix = rawPath === this.path ? "" : rawPath.slice(this.path.length + 1);
      const purpose = uploading ? "upload" : method === "GET" ? "download" : undefined;
      // Unauthorized requests are rejected without filesystem access or body consumption.
      const ticket = !masterAuthorized && bearer && purpose && suffix
        ? this.store.redeemTicket(suffix.toLowerCase(), purpose, bearer)
        : undefined;
      if (!masterAuthorized && !ticket) {
        res.setHeader("WWW-Authenticate", 'Bearer realm="ssh-mcp-server"');
        throw new StagingError("TRANSFER_UNAUTHORIZED", "Unauthorized.", 401);
      }
      if ((rawPath !== this.path && !suffix) || (!suffix && method !== "POST") ||
          (!uploading && method !== "GET" && method !== "DELETE")) {
        res.setHeader("Allow", suffix ? "GET, PUT, POST, DELETE" : "POST");
        throw new StagingError("TRANSFER_METHOD_NOT_ALLOWED", "Method not allowed.", 405);
      }
      const id = suffix ? normalizeTransferId(suffix) : randomUUID();

      if (expectContinue && !uploading) {
        res.setHeader("Connection", "close");
      }
      if (uploading) {
        const { "content-length": length, "x-file-name": name } = req.headers;
        const expected = length === undefined ? undefined : Number(length);
        if (length !== undefined && (!/^\d+$/.test(length) || !Number.isSafeInteger(expected))) {
          throw new StagingError("FILE_TOO_LARGE", "Content-Length exceeds the supported file size.", 413);
        }
        const fileName = typeof name === "string" ? name : ticket?.fileName ?? this.store.preparedFileName(id);
        const metadata = await this.receive(req, res, expectContinue, id, fileName, expected);
        json(res, 201, metadata);
      } else if (method === "DELETE") {
        json(res, 200, this.store.cleanup(id));
      } else {
        const { searchParams } = new URL(req.url ?? "/", "http://127.0.0.1");
        const consume = searchParams.get("consume");
        const deleteAfter = consume === "1" || consume === "true" || (consume === null && ticket?.consume === true);
        await this.store.withFile(id, async (file, _filePath, stream) => {
          if (res.destroyed) {
            return;
          }
          res.writeHead(200, {
            "Content-Type": "application/octet-stream",
            "Content-Length": file.sizeBytes,
            "X-Content-Sha256": file.sha256,
            "Cache-Control": "no-store",
            "Content-Disposition": contentDisposition(file.fileName),
          });
          await pipeline(stream(), res);
          if (deleteAfter && res.writableFinished && !res.writableAborted) {
            this.store.cleanup(id);
          }
        });
      }
    } catch (error) {
      if (res.destroyed || res.headersSent) {
        return;
      }
      if (!req.complete || expectContinue) {
        res.setHeader("Connection", "close");
      }
      if (error instanceof StagingError) {
        json(res, error.status, { code: error.code, message: error.message });
      } else {
        Logger.log(`Staging request failed: ${(error as Error).message}`, "error");
        json(res, 500, { code: "UNKNOWN_ERROR", message: "Staging request failed." });
      }
    }
  }

  private async receive(
    req: IncomingMessage,
    res: ServerResponse,
    expectContinue: boolean,
    id: string,
    fileName: string | undefined,
    expected: number | undefined,
  ) {
    // Pipeline errors must not destroy the HTTP socket before we can send 413/507.
    const body = new PassThrough();
    const incomplete = () => new StagingError("TRANSFER_INCOMPLETE", "Request body was interrupted.", 400);
    const onError = () => body.destroy(incomplete());
    const signal = AbortSignal.timeout(UPLOAD_TIMEOUT_MS);
    req.once("aborted", onError);
    req.on("error", onError);
    try {
      return await this.store.save(id, fileName, expected, {
        stream: () => body,
        signal,
        start: () => {
          // The store has reserved capacity and attached the pipeline before reading.
          if (expectContinue) {
            res.writeContinue();
          }
          if (req.aborted || req.destroyed) {
            onError();
          } else {
            req.pipe(body);
          }
        },
        assertComplete: () => {
          if (!req.complete || req.aborted) {
            throw incomplete();
          }
        },
      });
    } catch (error) {
      if (signal.aborted) {
        throw new StagingError("OPERATION_TIMEOUT", "Upload timed out.", 408);
      }
      throw error;
    } finally {
      req.unpipe(body);
      body.destroy();
      req.off("aborted", onError);
      req.off("error", onError);
    }
  }
}
