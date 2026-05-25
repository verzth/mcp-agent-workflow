package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
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

	ctx := context.Background()

	// MCP Tools
	mcpTools := tools.New(cfg.MCP.BridgeGRPCAddr, cfg.MCP.APIKey, logger)

	// Start background stream listener (zero tokens)
	mcpTools.StartStreamListener(ctx)

	s := server.NewMCPServer("agent-workflow-mcp", "1.0.0")

	publishTool := mcp.NewTool("publish_task",
		mcp.WithDescription("Publish a task to Agent Workflow"),
		mcp.WithString("objective", mcp.Required(), mcp.Description("What Agent should accomplish")),
		mcp.WithString("context", mcp.Description("Additional context for task")),
		mcp.WithString("repo", mcp.Required(), mcp.Description("Repository name or slug")),
		mcp.WithString("base_branch", mcp.Description("Base branch, default main")),
		mcp.WithString("work_branch", mcp.Description("Work branch name")),
		mcp.WithArray("constraints", mcp.Description("Rules agent must follow")),
		mcp.WithString("mr_target_repo", mcp.Description("Target repo for MR")),
		mcp.WithString("mr_target_branch", mcp.Description("Target branch for MR")),
		mcp.WithString("mr_title", mcp.Description("Merge request title")),
	)

	s.AddTool(publishTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		params := tools.PublishTaskParams{}
		b, _ := json.Marshal(args)
		_ = json.Unmarshal(b, &params)
		result, err := mcpTools.PublishTask(ctx, params)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	statusTool := mcp.NewTool("get_task_status",
		mcp.WithDescription("Get task status by task ID"),
		mcp.WithString("task_id", mcp.Required(), mcp.Description("Task ID")),
	)

	s.AddTool(statusTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		taskID, _ := req.RequireString("task_id")
		result, err := mcpTools.GetTaskStatus(ctx, taskID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	pendingTool := mcp.NewTool("get_pending_tasks", mcp.WithDescription("List pending tasks from MCP cache"))
	s.AddTool(pendingTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := mcpTools.GetPendingTasks(ctx)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	logger.Info("mcp server ready",
		zap.String("bridge", cfg.MCP.BridgeGRPCAddr),
	)

	if err := server.ServeStdio(s); err != nil {
		logger.Fatal("mcp stdio server failed", zap.Error(err))
	}
}
