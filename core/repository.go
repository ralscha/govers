package core

import "context"

// Repository defines the storage interface for govers.
// Implementations can store snapshots and commits in various backends
// (in-memory, SQL databases, MongoDB, etc.)
type Repository interface {
	// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
	GetHeadID(ctx context.Context) (CommitID, error)

	// Persist saves a commit and its snapshots to the repository.
	Persist(ctx context.Context, commit Commit) error

	// GetLatestSnapshot returns the most recent snapshot for the given GlobalID.
	// Returns nil, nil if no snapshot exists for this GlobalID.
	GetLatestSnapshot(ctx context.Context, globalID GlobalID) (*Snapshot, error)

	// GetSnapshots returns snapshots matching the given query.
	GetSnapshots(ctx context.Context, query Query) ([]Snapshot, error)

	// GetSnapshot returns a specific snapshot by GlobalID and version.
	// Returns nil, nil if no such snapshot exists.
	GetSnapshot(ctx context.Context, globalID GlobalID, version int64) (*Snapshot, error)
}
