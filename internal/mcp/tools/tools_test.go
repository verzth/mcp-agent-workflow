package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestPublishTask_OfflineReturnsSafeMessage(t *testing.T) {
	m := New("127.0.0.1:1", "dummy-key", nil, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err := m.PublishTask(ctx, PublishTaskParams{
		Objective: "test objective",
		Repo:      "test-repo",
	})
	if err != nil {
		t.Fatalf("PublishTask returned error in offline mode: %v", err)
	}
	if !strings.Contains(msg, "Bridge unavailable") && !strings.Contains(msg, "Publish failed") {
		t.Fatalf("unexpected offline publish message: %q", msg)
	}
}

func TestGetTaskStatus_OfflineReturnsSafeJSON(t *testing.T) {
	m := New("127.0.0.1:1", "dummy-key", nil, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err := m.GetTaskStatus(ctx, "task-123")
	if err != nil {
		t.Fatalf("GetTaskStatus returned error in offline mode: %v", err)
	}
	if !strings.Contains(msg, "\"task_id\": \"task-123\"") {
		t.Fatalf("missing task_id in status response: %q", msg)
	}
	if !strings.Contains(msg, "offline") && !strings.Contains(msg, "unknown") {
		t.Fatalf("missing offline/unknown fallback status: %q", msg)
	}
}
