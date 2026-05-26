# mcp-agent-workflow

MCP server for Agent Workflow bridge.

## Config

Create `config.yaml`:

```yaml
mcp:
  bridge_grpc_addr: "bridge-grpc.yggdrasil.verzth.work:443"
  api_key: "hpk_..."
  topics:
    - "sometopic"
    - "global.status.*"
```

Topic behavior:

- A shorthand topic like `sometopic` is normalized to `global.sometopic`.
- Fully-qualified topics like `global.status.*` are kept as-is.
- If `topics` is empty, default is `global.>`.

## Run modes

### STDIO mode (default)

For Claude MCP integration:

```bash
CONFIG_PATH=./config.yaml go run ./cmd/mcp/main.go
```

### Daemon mode

For long-running stream validation without stdio transport:

```bash
MCP_DAEMON=1 CONFIG_PATH=./config.yaml go run ./cmd/mcp/main.go
```

In daemon mode, process stays alive until `SIGINT`/`SIGTERM`.
