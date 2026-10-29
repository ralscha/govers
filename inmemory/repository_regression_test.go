package inmemory_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ralscha/govers/core"
	"github.com/ralscha/govers/inmemory"
)

func TestPersistValidatesAllVersionsBeforeWriting(t *testing.T) {
	ctx := context.Background()
	repo := inmemory.New()
	metadata := core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author")
	first := core.NewSnapshot(core.NewInstanceID("Employee", 1), core.EmptySnapshotState(), core.Initial, 1, metadata)
	duplicate := core.NewCommit(metadata).WithSnapshot(first).WithSnapshot(first)
	if err := repo.Persist(ctx, duplicate); !errors.Is(err, core.ErrConcurrentCommit) {
		t.Fatalf("expected duplicate version conflict, got %v", err)
	}
	if repo.Count() != 0 {
		t.Fatal("failed commit persisted snapshots")
	}
	head, err := repo.GetHeadID(ctx)
	if err != nil || !head.IsZero() {
		t.Fatalf("failed commit changed head: %v, %v", head, err)
	}

	second := core.NewSnapshot(first.GlobalID, core.EmptySnapshotState(), core.Update, 2, metadata)
	if err := repo.Persist(ctx, core.NewCommit(metadata).WithSnapshot(first).WithSnapshot(second)); err != nil {
		t.Fatalf("consecutive versions in one commit: %v", err)
	}
	for _, version := range []int64{1, 2, 3} {
		snapshot, err := repo.GetSnapshot(ctx, first.GlobalID, version)
		if err != nil {
			t.Fatal(err)
		}
		if version == 3 {
			if snapshot != nil {
				t.Fatal("found a version that was not persisted")
			}
		} else if snapshot == nil || snapshot.Version != version {
			t.Fatalf("version %d was not preserved: %v", version, snapshot)
		}
	}
}

func TestPaginationIsStableWithinOneCommit(t *testing.T) {
	ctx := context.Background()
	repo := inmemory.New()
	metadata := core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author")
	commit := core.NewCommit(metadata)
	want := []string{"Employee/1", "Employee/2", "Employee/3", "Employee/4"}
	for _, id := range []int{4, 1, 3, 2} {
		commit = commit.WithSnapshot(core.NewSnapshot(core.NewInstanceID("Employee", id), core.EmptySnapshotState(), core.Initial, 1, metadata))
	}
	if err := repo.Persist(ctx, commit); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		got := make([]string, 0, len(want))
		for offset := range len(want) {
			snapshots, err := repo.GetSnapshots(ctx, core.AnyDomainObjectQuery().Skip(offset).Limit(1).Build())
			if err != nil || len(snapshots) != 1 {
				t.Fatalf("page %d: %v, %v", offset, snapshots, err)
			}
			got = append(got, snapshots[0].GlobalID.Value())
		}
		if !slices.Equal(got, want) {
			t.Fatalf("unstable pagination: got %v, want %v", got, want)
		}
	}
}

func TestQueryBySnapshotType(t *testing.T) {
	ctx := context.Background()
	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))
	employee := Employee{ID: 1, FirstName: "Alice"}
	if _, err := g.Commit(ctx, "author", employee); err != nil {
		t.Fatal(err)
	}
	employee.FirstName = "Bob"
	if _, err := g.Commit(ctx, "author", employee); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Delete(ctx, "author", employee); err != nil {
		t.Fatal(err)
	}
	for _, snapshotType := range []core.SnapshotType{core.Initial, core.Update, core.Terminal} {
		t.Run(string(snapshotType), func(t *testing.T) {
			for _, query := range []*core.QueryBuilder{
				core.AnyDomainObjectQuery(), core.ByClassQuery("Employee"), core.ByInstanceIDQuery("Employee", 1),
			} {
				snapshots, err := repo.GetSnapshots(ctx, query.WithSnapshotType(snapshotType).Build())
				if err != nil || len(snapshots) != 1 {
					t.Fatalf("snapshot type query: %v, %v", snapshots, err)
				}
				if snapshots[0].Type != snapshotType {
					t.Fatalf("got type %s, want %s", snapshots[0].Type, snapshotType)
				}
			}
		})
	}
}

func TestPersistCopiesGlobalIDPointers(t *testing.T) {
	ctx := context.Background()
	repo := inmemory.New()
	id := core.NewInstanceID("Employee", 1)
	metadata := core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author")
	snapshot := core.NewSnapshot(&id, core.EmptySnapshotState(), core.Initial, 1, metadata)
	if err := repo.Persist(ctx, core.NewCommit(metadata).WithSnapshot(snapshot)); err != nil {
		t.Fatal(err)
	}
	id = core.NewInstanceID("Changed", 2)
	stored, err := repo.GetLatestSnapshot(ctx, core.NewInstanceID("Employee", 1))
	if err != nil || stored == nil {
		t.Fatalf("snapshot lookup: %v, %v", stored, err)
	}
	if stored.GlobalID.Value() != "Employee/1" {
		t.Fatalf("snapshot ID was changed through caller pointer: %s", stored.GlobalID.Value())
	}
}

func BenchmarkGetSnapshotLargeHistory(b *testing.B) {
	ctx := context.Background()
	repo := inmemory.New()
	id := core.NewInstanceID("Employee", 1)
	for version := int64(1); version <= 10000; version++ {
		metadata := core.NewCommitMetadata(core.CommitID{MajorID: version}, "author")
		snapshot := core.NewSnapshot(id, core.EmptySnapshotState(), core.Update, version, metadata)
		if err := repo.Persist(ctx, core.NewCommit(metadata).WithSnapshot(snapshot)); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for b.Loop() {
		snapshot, err := repo.GetSnapshot(ctx, id, 10000)
		if err != nil || snapshot == nil {
			b.Fatalf("snapshot lookup failed: %v", err)
		}
	}
}
