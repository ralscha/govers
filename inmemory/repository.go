// Package inmemory provides an in-memory implementation of the govers Repository interface.
// This is useful for testing and development, but not recommended for production use
// as data is lost when the application stops.
package inmemory

import (
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
	return r.headID, nil
}

// Persist saves a commit and its snapshots to the repository.
func (r *Repository) Persist(ctx context.Context, commit core.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	expectedID := r.headID.Next()
	if commit.Metadata.ID != expectedID {
		return fmt.Errorf("%w: expected commit ID %s, got %s", core.ErrConcurrentCommit, expectedID, commit.Metadata.ID)
	}
	for _, snapshot := range commit.Snapshots {
		history := r.snapshots[snapshot.GlobalID.Value()]
		expectedVersion := int64(1)
		if len(history) > 0 {
			expectedVersion = history[len(history)-1].Version + 1
		}
		if snapshot.Version != expectedVersion {
			return fmt.Errorf("%w: expected snapshot version %d for %s, got %d", core.ErrConcurrentCommit, expectedVersion, snapshot.GlobalID.Value(), snapshot.Version)
		}
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

	var results []core.Snapshot

	switch query.Type {
	case core.QueryByInstanceID:
		if query.InstanceID != nil {
			key := query.InstanceID.Value()
			if snapshots, exists := r.snapshots[key]; exists {
				results = r.filterSnapshots(snapshots, query)
			}
		}

	case core.QueryByClass:
		for _, snapshots := range r.snapshots {
			for _, s := range snapshots {
				if s.GlobalID.TypeName() == query.TypeName {
					results = append(results, s)
				}
			}
		}
		results = r.filterSnapshots(results, query)

	case core.QueryAny:
		for _, snapshots := range r.snapshots {
			results = append(results, snapshots...)
		}
		results = r.filterSnapshots(results, query)
	}

	slices.SortFunc(results, func(a, b core.Snapshot) int {
		if !a.CommitMetadata.CommitDate.Equal(b.CommitMetadata.CommitDate) {
			if a.CommitMetadata.CommitDate.After(b.CommitMetadata.CommitDate) {
				return -1
			}
			return 1
		}
		if a.CommitMetadata.ID.MajorID != b.CommitMetadata.ID.MajorID {
			if a.CommitMetadata.ID.MajorID > b.CommitMetadata.ID.MajorID {
				return -1
			}
			return 1
		}
		if a.CommitMetadata.ID.MinorID != b.CommitMetadata.ID.MinorID {
			if a.CommitMetadata.ID.MinorID > b.CommitMetadata.ID.MinorID {
				return -1
			}
			return 1
		}

		if a.Version > b.Version {
			return -1
		}
		if a.Version < b.Version {
			return 1
		}
		return 0
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

	key := globalID.Value()
	snapshots, exists := r.snapshots[key]
	if !exists {
		return nil, nil
	}

	for i := range snapshots {
		if snapshots[i].Version == version {
			snapshot := cloneSnapshot(snapshots[i])
			return &snapshot, nil
		}
	}

	return nil, nil
}

func (r *Repository) filterSnapshots(snapshots []core.Snapshot, query core.Query) []core.Snapshot {
	filtered := make([]core.Snapshot, 0, len(snapshots))

	for _, s := range snapshots {
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
