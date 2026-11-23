import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { SSHConnectionManager } from "../services/ssh-connection-manager.js";
import { Logger } from "../utils/logger.js";
import { stagingToolResult, uploadStagedTransfer, type StagingTools } from "../staging/staging-tools.js";
import { toToolError, ToolError } from "../utils/tool-error.js";

/**
 * Register file upload tool
 */
export function registerUploadTool(server: McpServer, staging?: StagingTools): void {
  const sshManager = SSHConnectionManager.getInstance();

  server.registerTool(
    "upload",
    {
      description: "Upload a file, or a whole directory (recursively; symlinks skipped, max 2000 files), to connected server. Provide either localPath or an HTTP staging transferId.",
      inputSchema: {
        localPath: z.string().optional().describe("Local path"),
        transferId: z.string().optional().describe("ID of a file received through HTTP staging"),
        verifySha256: z.boolean().optional().describe("For staged files, verify SHA-256 over SFTP"),
        cleanupAfterUpload: z.boolean().optional().describe("Remove a staged file after successful upload (default true)"),
        remotePath: z.string().describe("Remote path (POSIX absolute like /home/user/file.txt, or Windows drive-absolute like C:/Users/file.txt)"),
        connectionName: z.string().optional().describe("SSH connection name (optional, default is 'default')"),
      },
    },
    async ({ localPath, transferId, remotePath, connectionName, verifySha256, cleanupAfterUpload }) => {
      if ((localPath === undefined) === (transferId === undefined)) {
        return stagingToolResult(() => {
          throw new ToolError("INVALID_TRANSFER_ARGUMENTS", "Specify exactly one of localPath and transferId.");
        });
      }
      if (transferId !== undefined) {
        return stagingToolResult(() => uploadStagedTransfer(staging, sshManager, {
          transferId, remotePath, connectionName, verifySha256, cleanupAfterUpload,
        }));
      }
      try {
        const result = await sshManager.upload(localPath!, remotePath, connectionName);
        return {
          content: [{ type: "text", text: result }],
        };
      } catch (error: unknown) {
        const toolError = toToolError(error, "UNKNOWN_ERROR");
        Logger.handleError(toolError, "Failed to upload file");
        return {
          content: [{
            type: "text",
            text: JSON.stringify(
              {
                code: toolError.code,
                message: toolError.message,
                retriable: toolError.retriable,
              },
              null,
              2,
            ),
          }],
          isError: true,
        };
      }
    }
  );
}
