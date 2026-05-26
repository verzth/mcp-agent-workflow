package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	bridgev1 "github.com/verzth/agent-workflow-contracts/bridge/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 10 * time.Second

// MCPTools provides MCP tool implementations for Claude Code
// Background stream listener runs zero tokens
// Tools are invoked only when Claude Code calls them
type MCPTools struct {
	bridgeAddr string
	apiKey     string
	topics     []string
	logger     *zap.Logger

	// Background state — populated by gRPC stream, zero tokens
	mu           sync.RWMutex
	pendingTasks []TaskStatus
	completedLog []TaskStatus
}

type TaskStatus struct {
	TaskID    string `json:"task_id"`
	Status    string `json:"status"`
	Objective string `json:"objective"`
	Result    string `json:"result,omitempty"`
	MRUrl     string `json:"mr_url,omitempty"`
}

func New(bridgeAddr, apiKey string, topics []string, logger *zap.Logger) *MCPTools {
	if len(topics) == 0 {
		topics = []string{"global.>"}
	}
	for i := range topics {
		topics[i] = normalizeTopicFilter(topics[i])
	}
	return &MCPTools{
		bridgeAddr:   bridgeAddr,
		apiKey:       apiKey,
		topics:       topics,
		logger:       logger,
		pendingTasks: make([]TaskStatus, 0),
		completedLog: make([]TaskStatus, 0),
	}
}

func normalizeTopicFilter(topic string) string {
	t := strings.TrimSpace(topic)
	if t == "" {
		return "global.>"
	}
	if strings.Contains(t, ".") {
		return t
	}
	return "global." + t
}

// --- Background Stream Listener (zero tokens) ---

// StartStreamListener connects to bridge gRPC and receives task status updates
// This runs as a background goroutine — pure Go, no LLM calls
func (m *MCPTools) StartStreamListener(ctx context.Context) {
	go func() {
		m.logger.Info("mcp: background stream listener started (0 tokens)")
		for _, topic := range m.topics {
			topic := topic
			go func() {
				backoff := 2 * time.Second
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if err := m.consumeTaskStream(ctx, topic); err != nil {
						m.logger.Warn("mcp: stream disconnected, retrying",
							zap.String("topic", topic),
							zap.Error(err),
							zap.Duration("retry_in", backoff),
						)
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
						if backoff < 30*time.Second {
							backoff = backoff * 2
							if backoff > 30*time.Second {
								backoff = 30 * time.Second
							}
						}
					}
				}
			}()
		}

		<-ctx.Done()
		m.logger.Info("mcp: background stream listener stopped")
	}()
}

func (m *MCPTools) consumeTaskStream(ctx context.Context, topic string) error {
	dialCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, m.bridgeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial bridge: %w", err)
	}
	defer conn.Close()

	client := bridgev1.NewBridgeServiceClient(conn)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	stream, err := client.StreamTasks(streamCtx, &bridgev1.StreamTasksRequest{
		ApiKey:        m.apiKey,
		SubjectFilter: topic,
	})
	if err != nil {
		return fmt.Errorf("start stream: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv stream: %w", err)
		}
		m.updateTaskStatus(event)
	}
}

func (m *MCPTools) updateTaskStatus(event *bridgev1.StreamTasksResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if event == nil {
		return
	}

	status := TaskStatus{TaskID: event.GetTaskId()}
	status.Status = event.GetStatus()
	status.Result = event.GetResult()
	status.MRUrl = event.GetMrUrl()
	if p := event.GetPayload(); p != nil {
		status.Objective = p.GetObjective()
	}
	if status.Status == "" {
		status.Status = "pending"
	}

	// Update or add to appropriate list
	if status.Status == "completed" || status.Status == "failed" {
		m.completedLog = append(m.completedLog, status)
		// Remove from pending
		for i, t := range m.pendingTasks {
			if t.TaskID == status.TaskID {
				m.pendingTasks = append(m.pendingTasks[:i], m.pendingTasks[i+1:]...)
				break
			}
		}
	} else {
		// Add to pending if new
		found := false
		for i, t := range m.pendingTasks {
			if t.TaskID == status.TaskID {
				m.pendingTasks[i] = status
				found = true
				break
			}
		}
		if !found {
			m.pendingTasks = append(m.pendingTasks, status)
		}
	}
}

// --- MCP Tool Definitions ---
// These are called by Claude Code — tokens consumed per call

// PublishTask creates and publishes a task to Hermes
// MCP tool name: "publish_task"
func (m *MCPTools) PublishTask(ctx context.Context, params PublishTaskParams) (string, error) {
	m.logger.Info("mcp tool: publish_task called",
		zap.String("objective", params.Objective),
		zap.String("repo", params.Repo),
	)

	if params.BaseBranch == "" {
		params.BaseBranch = "main"
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	conn, err := grpc.DialContext(rpcCtx, m.bridgeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Sprintf("Bridge unavailable. Task not published yet.\nObjective: %s\nRepo: %s\nReason: %v", params.Objective, params.Repo, err), nil
	}
	defer conn.Close()

	client := bridgev1.NewBridgeServiceClient(conn)
	resp, err := client.PublishTask(rpcCtx, &bridgev1.PublishTaskRequest{
		ApiKey:  m.apiKey,
		Subject: fmt.Sprintf("global.%s", params.Repo),
		Payload: &bridgev1.TaskPayload{
			Objective:   params.Objective,
			Context:     params.Context,
			Repo:        params.Repo,
			BaseBranch:  params.BaseBranch,
			WorkBranch:  params.WorkBranch,
			Constraints: params.Constraints,
			MrTarget: &bridgev1.MergeRequestTarget{
				Repo:   params.MRTargetRepo,
				Branch: params.MRTargetBranch,
				Title:  params.MRTitle,
			},
		},
	})
	if err != nil {
		return fmt.Sprintf("Publish failed (bridge reachable but request failed).\nObjective: %s\nRepo: %s\nReason: %v", params.Objective, params.Repo, err), nil
	}
	taskID := resp.GetTaskId()

	// Track in pending
	m.mu.Lock()
	m.pendingTasks = append(m.pendingTasks, TaskStatus{
		TaskID:    taskID,
		Status:    "pending",
		Objective: params.Objective,
	})
	m.mu.Unlock()

	return fmt.Sprintf("Task published: %s\nObjective: %s\nRepo: %s\nBranch: %s",
		taskID, params.Objective, params.Repo, params.WorkBranch), nil
}

// GetTaskStatus queries the status of a specific task
// MCP tool name: "get_task_status"
func (m *MCPTools) GetTaskStatus(ctx context.Context, taskID string) (string, error) {
	m.logger.Info("mcp tool: get_task_status called", zap.String("task_id", taskID))

	// Check local cache first (from stream listener)
	m.mu.RLock()
	for _, t := range m.pendingTasks {
		if t.TaskID == taskID {
			m.mu.RUnlock()
			result, _ := json.MarshalIndent(t, "", "  ")
			return string(result), nil
		}
	}
	for _, t := range m.completedLog {
		if t.TaskID == taskID {
			m.mu.RUnlock()
			result, _ := json.MarshalIndent(t, "", "  ")
			return string(result), nil
		}
	}
	m.mu.RUnlock()

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	conn, err := grpc.DialContext(rpcCtx, m.bridgeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Sprintf(`{"task_id": %q, "status": "offline", "reason": %q}`, taskID, err.Error()), nil
	}
	defer conn.Close()

	client := bridgev1.NewBridgeServiceClient(conn)
	resp, err := client.GetTaskStatus(rpcCtx, &bridgev1.GetTaskStatusRequest{
		ApiKey: m.apiKey,
		TaskId: taskID,
	})
	if err != nil {
		return fmt.Sprintf(`{"task_id": %q, "status": "unknown", "reason": %q}`, taskID, err.Error()), nil
	}

	result, _ := json.MarshalIndent(map[string]interface{}{
		"task_id": resp.GetTaskId(),
		"status":  resp.GetStatus(),
		"result":  resp.GetResult(),
		"mr_url":  resp.GetMrUrl(),
	}, "", "  ")
	return string(result), nil
}

// GetPendingTasks returns all currently pending tasks
// MCP tool name: "get_pending_tasks"
func (m *MCPTools) GetPendingTasks(ctx context.Context) (string, error) {
	m.logger.Info("mcp tool: get_pending_tasks called")

	m.mu.RLock()
	defer m.mu.RUnlock()

	result, _ := json.MarshalIndent(map[string]interface{}{
		"pending_count":   len(m.pendingTasks),
		"pending_tasks":   m.pendingTasks,
		"completed_count": len(m.completedLog),
	}, "", "  ")

	return string(result), nil
}

// --- MCP Tool Parameter Types ---

type PublishTaskParams struct {
	Objective    string   `json:"objective" description:"What Hermes should accomplish"`
	Context      string   `json:"context" description:"Additional context for the task"`
	Repo         string   `json:"repo" description:"Repository path in ai-workspace (e.g. nav-engine-v2)"`
	BaseBranch   string   `json:"base_branch" description:"Base branch to work from (default: main)"`
	WorkBranch   string   `json:"work_branch" description:"Branch name for AI work (e.g. ai/fix-nav-decimal)"`
	Constraints  []string `json:"constraints" description:"Rules the AI must follow"`
	MRTargetRepo   string `json:"mr_target_repo" description:"Target repo for MR (root group repo)"`
	MRTargetBranch string `json:"mr_target_branch" description:"Target branch for MR (default: main)"`
	MRTitle      string   `json:"mr_title" description:"Merge request title"`
}

// --- MCP Server Registration ---
// Use with mcp-go SDK:
//
// server := mcp.NewServer("hermes-mcp")
// server.AddTool("publish_task", "Publish a task to Hermes for autonomous execution", publishTaskSchema, tools.PublishTask)
// server.AddTool("get_task_status", "Get status of a Hermes task", statusSchema, tools.GetTaskStatus)
// server.AddTool("get_pending_tasks", "List all pending and completed tasks", nil, tools.GetPendingTasks)
