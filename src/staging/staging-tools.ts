import path from "node:path";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { SSHConnectionManager } from "../services/ssh-connection-manager.js";
import { toToolError, ToolError } from "../utils/tool-error.js";
import type { StagingStore } from "./staging-store.js";

export interface StagingTools {
  store: StagingStore;
  publicBaseUrl: string;
}

function requireStaging(staging?: StagingTools): StagingTools {
  if (!staging) {
    throw new ToolError("HTTP_TRANSPORT_REQUIRED", "File staging requires --mcp-transport http.");
  }
  return staging;
}

export async function stagingToolResult(run: () => Promise<unknown> | unknown) {
  try {
    const value = await run();
    return { content: [{ type: "text" as const, text: JSON.stringify(value) }] };
  } catch (error) {
    const { code, message, retriable } = toToolError(error, "UNKNOWN_ERROR");
    return { content: [{ type: "text" as const, text: JSON.stringify({ code, message, retriable }) }], isError: true };
  }
}

export function registerStagingTools(server: McpServer, staging?: StagingTools): void {
  server.registerTool("prepare-upload", {
    description: 'Reserve an upload ticket. Run curl --fail -X PUT -H "Authorization: Bearer <transferToken>" --upload-file <localFile> "<uploadUrl>", then call upload with transferId.',
    inputSchema: {
      fileName: z.string().optional(),
      expectedSizeBytes: z.number().int().nonnegative().optional(),
    },
  }, ({ fileName, expectedSizeBytes }) => stagingToolResult(() => {
    const { store, publicBaseUrl } = requireStaging(staging);
    const ticket = store.prepareUpload(fileName, expectedSizeBytes);
    return {
      ...ticket,
      method: "PUT",
      uploadUrl: `${publicBaseUrl}/files/${ticket.transferId}`,
      maxFileBytes: store.maxFileBytes,
    };
  }));

  server.registerTool("cleanup-transfer", {
    description: "Revoke a transfer ticket and remove its staged file. Deletion waits for any active reads to finish.",
    inputSchema: { transferId: z.string() },
  }, ({ transferId }) => stagingToolResult(() => requireStaging(staging).store.cleanup(transferId)));
}

export async function uploadStagedTransfer(
  staging: StagingTools | undefined,
  sshManager: SSHConnectionManager,
  options: {
    transferId: string; remotePath: string; connectionName?: string;
    verifySha256?: boolean; cleanupAfterUpload?: boolean;
  },
) {
  const { store } = requireStaging(staging);
  const { transferId, remotePath, connectionName, verifySha256, cleanupAfterUpload } = options;
  return store.withFile(transferId, async (file, filePath, stream) => {
    const { sizeBytes, sha256 } = file;
    await sshManager.uploadStagedFile(filePath, remotePath, { sizeBytes, sha256, verifySha256, stream }, connectionName);
    const cleanedUp = cleanupAfterUpload !== false && store.cleanup(file.transferId).deleted;
    return { transferId: file.transferId, remotePath, sizeBytes, sha256, verifiedSha256: verifySha256 === true, cleanedUp };
  });
}

export async function downloadStagedTransfer(
  staging: StagingTools | undefined,
  sshManager: SSHConnectionManager,
  options: { remotePath: string; connectionName?: string; consumeOnDownload?: boolean },
) {
  const { store, publicBaseUrl } = requireStaging(staging);
  const { remotePath, connectionName, consumeOnDownload } = options;
  const { transferId, fileName, sizeBytes, sha256, expiresAt } = await sshManager.downloadFileTo(
    remotePath,
    (size, source) => store.saveNew(path.posix.basename(remotePath.replace(/\\/g, "/")), size, source),
    connectionName,
  );
  return {
    transferId, fileName, remotePath, sizeBytes, sha256, fileExpiresAt: expiresAt,
    downloadUrl: `${publicBaseUrl}/files/${transferId}`,
    ...store.issueDownload(transferId, consumeOnDownload !== false),
  };
}
