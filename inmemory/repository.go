// Package inmemory provides an in-memory implementation of the govers Repository interface.
// This is useful for testing and development, but not recommended for production use
// as data is lost when the application stops.
package inmemory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ralscha/govers/core"
)

// Repository is an in-memory implementation of core.Repository.
// It stores all data in memory using maps and is thread-safe.
type Repository struct {
	mu        sync.RWMutex
	snapshots map[string][]core.Snapshot // key: GlobalID.Value()
	headID    core.CommitID
}

// New creates a new in-memory repository.
func New() *Repository {
	return &Repository{
		snapshots: make(map[string][]core.Snapshot),
	}
}

// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
func (r *Repository) GetHeadID(ctx context.Context) (core.CommitID, error) {
	if err := ctx.Err(); err != nil {
		return core.CommitID{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return core.CommitID{}, err
	}
	return r.headID, nil
}

// Persist saves a commit and its snapshots to the repository.
func (r *Repository) Persist(ctx context.Context, commit core.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	expectedID := r.headID.Next()
	if commit.Metadata.ID != expectedID {
		return fmt.Errorf("%w: expected commit ID %s, got %s", core.ErrConcurrentCommit, expectedID, commit.Metadata.ID)
	}
	versions := make(map[string]int64, len(commit.Snapshots))
	for _, snapshot := range commit.Snapshots {
		key := snapshot.GlobalID.Value()
		version, seen := versions[key]
		if !seen {
			history := r.snapshots[key]
			if len(history) > 0 {
				version = history[len(history)-1].Version
			}
		}
		expectedVersion := version + 1
		if snapshot.Version != expectedVersion {
			return fmt.Errorf("%w: expected snapshot version %d for %s, got %d", core.ErrConcurrentCommit, expectedVersion, key, snapshot.Version)
		}
		versions[key] = snapshot.Version
	}

	r.headID = commit.Metadata.ID

	for _, snapshot := range commit.Snapshots {
		key := snapshot.GlobalID.Value()
		r.snapshots[key] = append(r.snapshots[key], cloneSnapshot(snapshot))
	}

	return nil
}

// GetLatestSnapshot returns the most recent snapshot for the given GlobalID.
// Returns nil if no snapshot exists for this GlobalID.
func (r *Repository) GetLatestSnapshot(ctx context.Context, globalID core.GlobalID) (*core.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := globalID.Value()
	snapshots, exists := r.snapshots[key]
	if !exists || len(snapshots) == 0 {
		return nil, nil
	}

	latest := cloneSnapshot(snapshots[len(snapshots)-1])
	return &latest, nil
}

// GetSnapshots returns snapshots matching the given query.
func (r *Repository) GetSnapshots(ctx context.Context, query core.Query) ([]core.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var results []core.Snapshot

	switch query.Type {
	case core.QueryByInstanceID:
		results = appendMatchingSnapshots(results, r.snapshots[query.InstanceID.Value()], query)
	case core.QueryByClass, core.QueryAny:
		for _, snapshots := range r.snapshots {
			results = appendMatchingSnapshots(results, snapshots, query)
		}
	}

	slices.SortFunc(results, func(a, b core.Snapshot) int {
		if order := b.CommitMetadata.CommitDate.Compare(a.CommitMetadata.CommitDate); order != 0 {
			return order
		}
		if order := cmp.Compare(b.CommitMetadata.ID.MajorID, a.CommitMetadata.ID.MajorID); order != 0 {
			return order
		}
		if order := cmp.Compare(b.CommitMetadata.ID.MinorID, a.CommitMetadata.ID.MinorID); order != 0 {
			return order
		}
		if order := cmp.Compare(b.Version, a.Version); order != 0 {
			return order
		}
		return cmp.Compare(a.GlobalID.Value(), b.GlobalID.Value())
	})

	if query.Skip > 0 {
		if query.Skip >= len(results) {
			return []core.Snapshot{}, nil
		}
		results = results[query.Skip:]
	}

	if query.Limit > 0 && query.Limit < len(results) {
		results = results[:query.Limit]
	}
	for i := range results {
		results[i] = cloneSnapshot(results[i])
	}

	return results, nil
}

// GetSnapshot returns a specific snapshot by GlobalID and version.
// Returns nil, nil if no such snapshot exists.
func (r *Repository) GetSnapshot(ctx context.Context, globalID core.GlobalID, version int64) (*core.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if version <= 0 {
		return nil, core.ErrInvalidVersion
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := globalID.Value()
	snapshots := r.snapshots[key]
	if version > int64(len(snapshots)) {
		return nil, nil
	}

	// Persist guarantees contiguous versions starting at one.
	snapshot := cloneSnapshot(snapshots[version-1])
	return &snapshot, nil
}

func appendMatchingSnapshots(filtered, snapshots []core.Snapshot, query core.Query) []core.Snapshot {
	for _, s := range snapshots {
		if query.Type == core.QueryByClass && s.GlobalID.TypeName() != query.TypeName {
			continue
		}

		if query.SnapshotType != "" && s.Type != query.SnapshotType {
			continue
		}

		if query.Version > 0 && s.Version != query.Version {
			continue
		}

		if query.Author != "" && s.CommitMetadata.Author != query.Author {
			continue
		}

		if !query.CommitID.IsZero() && s.CommitMetadata.ID != query.CommitID {
			continue
		}

		if !query.FromDate.IsZero() && s.CommitMetadata.CommitDate.Before(query.FromDate) {
			continue
		}
		if !query.ToDate.IsZero() && s.CommitMetadata.CommitDate.After(query.ToDate) {
			continue
		}

		if query.ChangedProperty != "" {
			found := slices.Contains(s.ChangedProperties, query.ChangedProperty)
			if !found {
				continue
			}
		}

		filtered = append(filtered, s)
	}

	return filtered
}

// Clear removes all data from the repository.
func (r *Repository) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.snapshots = make(map[string][]core.Snapshot)
	r.headID = core.CommitID{}
}

func cloneSnapshot(snapshot core.Snapshot) core.Snapshot {
	// Global IDs are immutable values, but callers can pass pointers to them.
	// Store a value so assigning through a caller-owned pointer cannot rewrite history.
	switch id := snapshot.GlobalID.(type) {
	case *core.InstanceID:
		snapshot.GlobalID = *id
	case *core.ValueObjectID:
		snapshot.GlobalID = *id
	}
	snapshot.State = snapshot.State.Clone()
	snapshot.ChangedProperties = append([]string(nil), snapshot.ChangedProperties...)
	snapshot.CommitMetadata.Properties = maps.Clone(snapshot.CommitMetadata.Properties)
	return snapshot
}

// Count returns the total number of snapshots in the repository.
func (r *Repository) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, snapshots := range r.snapshots {
		count += len(snapshots)
	}
	return count
}

// Ensure Repository implements core.Repository
var _ core.Repository = (*Repository)(nil)
