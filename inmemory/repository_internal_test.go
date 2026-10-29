package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ralscha/govers/core"
)

type observedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *observedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func TestCancellationWhileWaitingForRepositoryLock(t *testing.T) {
	id := core.NewInstanceID("Employee", 1)
	metadata := core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author")
	commit := core.NewCommit(metadata).WithSnapshot(core.NewSnapshot(id, core.EmptySnapshotState(), core.Initial, 1, metadata))
	operations := map[string]func(context.Context, *Repository) error{
		"head": func(ctx context.Context, repo *Repository) error {
			_, err := repo.GetHeadID(ctx)
			return err
		},
		"latest": func(ctx context.Context, repo *Repository) error {
			_, err := repo.GetLatestSnapshot(ctx, id)
			return err
		},
		"version": func(ctx context.Context, repo *Repository) error {
			_, err := repo.GetSnapshot(ctx, id, 1)
			return err
		},
		"query": func(ctx context.Context, repo *Repository) error {
			_, err := repo.GetSnapshots(ctx, core.AnyDomainObjectQuery().Build())
			return err
		},
		"persist": func(ctx context.Context, repo *Repository) error {
			return repo.Persist(ctx, commit)
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			repo := New()
			baseContext, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &observedContext{Context: baseContext, checked: make(chan struct{})}
			repo.mu.Lock()
			result := make(chan error, 1)
			go func() { result <- operation(ctx, repo) }()
			<-ctx.checked
			cancel()
			repo.mu.Unlock()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("expected cancellation after waiting for lock, got %v", err)
			}
			if repo.Count() != 0 {
				t.Fatal("canceled operation wrote a snapshot")
			}
		})
	}
}
