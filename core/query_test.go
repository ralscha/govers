package core

import (
	"errors"
	"testing"
)

func TestSnapshotTypeQuery(t *testing.T) {
	for _, snapshotType := range []SnapshotType{"", Initial, Update, Terminal} {
		query := AnyDomainObjectQuery().WithSnapshotType(snapshotType).Build()
		if err := query.Validate(); err != nil {
			t.Fatalf("valid snapshot type %q rejected: %v", snapshotType, err)
		}
		if query.SnapshotType != snapshotType {
			t.Fatalf("snapshot type = %q, want %q", query.SnapshotType, snapshotType)
		}
	}
	query := AnyDomainObjectQuery().WithSnapshotType("invalid").Build()
	if err := query.Validate(); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("invalid snapshot type error = %v", err)
	}
}

func TestQueryRejectsNegativeCommitID(t *testing.T) {
	for _, id := range []CommitID{{MajorID: -1}, {MajorID: 1, MinorID: -1}} {
		query := AnyDomainObjectQuery().WithCommitID(id).Build()
		if err := query.Validate(); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("negative commit ID %v error = %v", id, err)
		}
	}
}
