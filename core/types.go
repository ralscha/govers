package core

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"
)

// SnapshotType indicates the type of change captured in a snapshot.
type SnapshotType string

const (
	// Initial indicates the first snapshot of an object.
	Initial SnapshotType = "INITIAL"
	// Update indicates an update to an existing object.
	Update SnapshotType = "UPDATE"
	// Terminal indicates the object was deleted.
	Terminal SnapshotType = "TERMINAL"
)

// CommitID uniquely identifies a commit with major and minor version numbers.
// The major ID is incremented for each new commit, while the minor ID is used
// for multiple snapshots within the same commit operation.
type CommitID struct {
	MajorID int64
	MinorID int
}

// String returns the string representation of the CommitID: "major.minor"
func (c CommitID) String() string {
	return fmt.Sprintf("%d.%d", c.MajorID, c.MinorID)
}

// ParseCommitID parses a CommitID string in "major.minor" format.
func ParseCommitID(s string) (CommitID, error) {
	major, minor, ok := strings.Cut(s, ".")
	if !ok {
		return CommitID{}, fmt.Errorf("invalid commit ID %q", s)
	}

	majorID, err := strconv.ParseInt(major, 10, 64)
	if err != nil {
		return CommitID{}, fmt.Errorf("invalid commit ID major part %q: %w", major, err)
	}

	minorID, err := strconv.Atoi(minor)
	if err != nil {
		return CommitID{}, fmt.Errorf("invalid commit ID minor part %q: %w", minor, err)
	}

	return CommitID{MajorID: majorID, MinorID: minorID}, nil
}

// IsZero returns true if this is the zero value CommitID.
func (c CommitID) IsZero() bool {
	return c.MajorID == 0 && c.MinorID == 0
}

// Next returns the next CommitID (increments major, resets minor to 0).
func (c CommitID) Next() CommitID {
	return CommitID{MajorID: c.MajorID + 1, MinorID: 0}
}

// CommitMetadata holds metadata about a commit operation.
type CommitMetadata struct {
	// ID is the unique identifier for this commit.
	ID CommitID

	// Author is the name/identifier of who made the commit.
	Author string

	// CommitDate is when the commit was created.
	CommitDate time.Time

	// Properties holds custom key-value metadata for the commit.
	Properties map[string]string
}

// NewCommitMetadata creates a new CommitMetadata with the given author.
func NewCommitMetadata(id CommitID, author string) CommitMetadata {
	return CommitMetadata{
		ID:         id,
		Author:     author,
		CommitDate: time.Now(),
		Properties: make(map[string]string),
	}
}

// WithProperty returns a copy of CommitMetadata with the given property added.
func (m CommitMetadata) WithProperty(key, value string) CommitMetadata {
	newProps := make(map[string]string, len(m.Properties)+1)
	maps.Copy(newProps, m.Properties)
	newProps[key] = value
	m.Properties = newProps
	return m
}

// WithProperties returns a copy of CommitMetadata with all properties added.
func (m CommitMetadata) WithProperties(properties map[string]string) CommitMetadata {
	newProps := make(map[string]string, len(m.Properties)+len(properties))
	maps.Copy(newProps, m.Properties)
	maps.Copy(newProps, properties)
	m.Properties = newProps
	return m
}

// Snapshot represents a historical state of a domain object at a point in time.
type Snapshot struct {
	// GlobalID uniquely identifies the object this snapshot is for.
	GlobalID GlobalID

	// State holds the normalized property-value map of the object at the time of the snapshot.
	// Entity references are "dehydrated" to GlobalID string values for direct comparison.
	State SnapshotState

	// ChangedProperties lists the property names that changed from the previous version.
	// Empty for Initial snapshots.
	ChangedProperties []string

	// Type indicates whether this is an Initial, Update, or Terminal snapshot.
	Type SnapshotType

	// Version is the per-object version number, starting at 1.
	Version int64

	// CommitMetadata holds information about the commit that created this snapshot.
	CommitMetadata CommitMetadata
}

// NewSnapshot creates a new snapshot for an object.
func NewSnapshot(globalID GlobalID, state SnapshotState, snapshotType SnapshotType, version int64, metadata CommitMetadata) Snapshot {
	return Snapshot{
		GlobalID:          globalID,
		State:             state.Clone(),
		ChangedProperties: nil,
		Type:              snapshotType,
		Version:           version,
		CommitMetadata:    cloneCommitMetadata(metadata),
	}
}

// WithChangedProperties returns a copy of Snapshot with the changed properties set.
func (s Snapshot) WithChangedProperties(props []string) Snapshot {
	s.ChangedProperties = append([]string(nil), props...)
	return s
}

// IsInitial returns true if this is the first snapshot of the object.
func (s Snapshot) IsInitial() bool {
	return s.Type == Initial
}

// IsTerminal returns true if this snapshot represents object deletion.
func (s Snapshot) IsTerminal() bool {
	return s.Type == Terminal
}

// Commit represents a completed commit operation containing one or more snapshots.
type Commit struct {
	// Metadata holds information about this commit.
	Metadata CommitMetadata

	// Snapshots contains the snapshots created in this commit.
	Snapshots []Snapshot

	// Changes contains the changes detected in this commit.
	Changes []Change
}

// NewCommit creates a new Commit with the given metadata.
func NewCommit(metadata CommitMetadata) Commit {
	return Commit{
		Metadata:  cloneCommitMetadata(metadata),
		Snapshots: make([]Snapshot, 0),
		Changes:   make([]Change, 0),
	}
}

// WithSnapshot returns a copy of Commit with the given snapshot added.
func (c Commit) WithSnapshot(snapshot Snapshot) Commit {
	snapshots := make([]Snapshot, len(c.Snapshots)+1)
	copy(snapshots, c.Snapshots)
	snapshots[len(c.Snapshots)] = snapshot
	c.Snapshots = snapshots
	return c
}

// WithChange returns a copy of Commit with the given change added.
func (c Commit) WithChange(change Change) Commit {
	changes := make([]Change, len(c.Changes)+1)
	copy(changes, c.Changes)
	changes[len(c.Changes)] = change
	c.Changes = changes
	return c
}

func cloneCommitMetadata(metadata CommitMetadata) CommitMetadata {
	metadata.Properties = maps.Clone(metadata.Properties)
	return metadata
}
