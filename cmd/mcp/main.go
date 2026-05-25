package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/yaml.v3"

	"github.com/verzth/mcp-agent-workflow/internal/mcp/tools"
	"go.uber.org/zap"
)

type mcpConfig struct {
	MCP struct {
		BridgeGRPCAddr string `yaml:"bridge_grpc_addr"`
		APIKey         string `yaml:"api_key"`
	} `yaml:"mcp"`
}

func loadMCPConfig(path string) (*mcpConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c mcpConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.MCP.BridgeGRPCAddr == "" {
		c.MCP.BridgeGRPCAddr = "localhost:50051"
	}
	if c.MCP.APIKey == "" {
		return nil, fmt.Errorf("mcp.api_key is required")
	}
	return &c, nil
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}

	cfg, err := loadMCPConfig(cfgPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// MCP Tools
	mcpTools := tools.New(cfg.MCP.BridgeGRPCAddr, cfg.MCP.APIKey, logger)

	// Start background stream listener (zero tokens)
	mcpTools.StartStreamListener(ctx)

	// Register MCP server using mcp-go SDK
	// server := mcp.NewServer("hermes-mcp", mcp.WithVersion("1.0.0"))
	//
	// server.AddTool(mcp.Tool{
	//     Name:        "publish_task",
	//     Description: "Publish a task to Hermes for autonomous code execution. Hermes will clone the repo, apply changes, and open an MR.",
	//     InputSchema: publishTaskSchema,
	// }, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	//     var params tools.PublishTaskParams
	//     json.Unmarshal(req.Params.Arguments, &params)
	//     result, err := mcpTools.PublishTask(ctx, params)
	//     return &mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: result}}}, err
	// })
	//
	// server.AddTool(mcp.Tool{
	//     Name:        "get_task_status",
	//     Description: "Get execution status of a Hermes task by ID",
	//     InputSchema: statusSchema,
	// }, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	//     taskID := req.Params.Arguments["task_id"].(string)
	//     result, err := mcpTools.GetTaskStatus(ctx, taskID)
	//     return &mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: result}}}, err
	// })
	//
	// server.AddTool(mcp.Tool{
	//     Name:        "get_pending_tasks",
	//     Description: "List all pending and recently completed Hermes tasks",
	// }, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	//     result, err := mcpTools.GetPendingTasks(ctx)
	//     return &mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: result}}}, err
	// })
	//
	// // Start as stdio transport (for Claude Code)
	// server.ServeStdio()

	_ = mcpTools

	logger.Info("mcp server ready",
		zap.String("bridge", cfg.MCP.BridgeGRPCAddr),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	logger.Info("mcp server stopped")
}
