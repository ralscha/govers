package mongodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/ralscha/govers/core"
)

func TestQueryStablePaginationAndSnapshotTypes(t *testing.T) {
	repo, cleanup := setupTestContainer(t)
	defer cleanup()
	ctx := context.Background()
	date := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for major := int64(1); major <= 10; major++ {
		metadata := core.NewCommitMetadata(core.CommitID{MajorID: major}, "author")
		metadata.CommitDate = date
		commit := core.NewCommit(metadata)
		ids := []string{"z", "a"} // Deliberately reverse the desired final order.
		snapshotType := core.Update
		version := major
		if major == 1 || major == 10 {
			snapshotType = core.Initial
		}
		if major == 9 {
			snapshotType = core.Terminal
		}
		if major == 10 {
			ids = []string{"z-new", "a-new"}
			version = 1
		}
		for _, id := range ids {
			commit = commit.WithSnapshot(core.NewSnapshot(core.NewInstanceID("Record", id),
				core.EmptySnapshotState(), snapshotType, version, metadata))
		}
		if err := repo.Persist(ctx, commit); err != nil {
			t.Fatal(err)
		}
	}
	for skip, want := range []string{"Record/a-new", "Record/z-new", "Record/a", "Record/z"} {
		snapshots, err := repo.GetSnapshots(ctx, core.AnyDomainObjectQuery().Skip(skip).Limit(1).Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 1 || snapshots[0].GlobalID.Value() != want {
			t.Fatalf("page %d: expected %s, got %v", skip, want, snapshots)
		}
	}
	for snapshotType, count := range map[core.SnapshotType]int{core.Initial: 4, core.Update: 14, core.Terminal: 2} {
		snapshots, err := repo.GetSnapshots(ctx, core.AnyDomainObjectQuery().WithSnapshotType(snapshotType).Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != count {
			t.Fatalf("type %s: expected %d snapshots, got %d", snapshotType, count, len(snapshots))
		}
		for _, snapshot := range snapshots {
			if snapshot.Type != snapshotType {
				t.Fatalf("type filter %s returned %s", snapshotType, snapshot.Type)
			}
		}
	}
}
