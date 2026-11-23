import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { SSHConnectionManager } from "../services/ssh-connection-manager.js";
import { Logger } from "../utils/logger.js";
import { downloadStagedTransfer, stagingToolResult, type StagingTools } from "../staging/staging-tools.js";
import { toToolError } from "../utils/tool-error.js";

/**
 * Register file download tool
 */
export function registerDownloadTool(server: McpServer, staging?: StagingTools): void {
  const sshManager = SSHConnectionManager.getInstance();

  server.registerTool(
    "download",
    {
      description: "Download a file, or a whole remote directory (recursively; symlinks skipped, max 2000 files), from connected server. Omit localPath to stage a single file for HTTP download. Run curl --fail -H \"Authorization: Bearer <transferToken>\" -o <destFile> \"<downloadUrl>\" and compare SHA-256.",
      inputSchema: {
        remotePath: z.string().describe("Remote path (POSIX absolute like /home/user/file.txt, or Windows drive-absolute like C:/Users/file.txt)"),
        localPath: z.string().optional().describe("Local path"),
        consumeOnDownload: z.boolean().optional().describe("Remove the staged file after a completed ticket download (default true)"),
        connectionName: z.string().optional().describe("SSH connection name (optional, default is 'default')"),
      },
    },
    async ({ remotePath, localPath, connectionName, consumeOnDownload }) => {
      if (localPath === undefined) {
        return stagingToolResult(() => downloadStagedTransfer(staging, sshManager, {
          remotePath, connectionName, consumeOnDownload,
        }));
      }
      try {
        const result = await sshManager.download(remotePath, localPath, connectionName);
        return {
          content: [{ type: "text", text: result }],
        };
      } catch (error: unknown) {
        const toolError = toToolError(error, "UNKNOWN_ERROR");
        Logger.handleError(toolError, "Failed to download file");
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
