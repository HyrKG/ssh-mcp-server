import { registerStagingTools, type StagingTools } from "../staging/staging-tools.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerExecuteCommandTool } from "./execute-command.js";
import { registerJobTools } from "./jobs.js";
import { registerUploadTool } from "./upload.js";
import { registerDownloadTool } from "./download.js";
import { registerListServersTool } from "./list-servers.js";
import { registerFileIoTools } from "./file-io.js";

/**
 * Register all tools
 * @param server MCP server instance
 */
export function registerAllTools(server: McpServer, staging?: StagingTools): void {
  registerExecuteCommandTool(server);
  registerJobTools(server);
  registerUploadTool(server, staging);
  registerDownloadTool(server, staging);
  registerListServersTool(server);
  registerFileIoTools(server);
  registerStagingTools(server, staging);
}
