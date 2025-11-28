package core

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Govers is the main entry point for the object versioning library.
// It provides methods to commit object changes and query historical data.
type Govers struct {
	repository      Repository
	snapshotFactory *SnapshotFactory
	mu              sync.Mutex
}

// Option is a functional option for configuring a Govers instance.
type Option func(*Govers)

// WithRepository sets the repository for storing snapshots and commits.
func WithRepository(repo Repository) Option {
	return func(g *Govers) {
		g.repository = repo
	}
}

// WithSnapshotFactory sets the snapshot factory for creating snapshots.
func WithSnapshotFactory(factory *SnapshotFactory) Option {
	return func(g *Govers) {
		g.snapshotFactory = factory
	}
}

// New creates a new Govers instance with the given options.
func New(opts ...Option) *Govers {
	g := &Govers{
		snapshotFactory: NewSnapshotFactory(),
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

// Commit saves a new version of the given object.
// If this is the first time the object is committed, an INITIAL snapshot is created.
// Otherwise, an UPDATE snapshot is created with the changes from the previous version.
func (g *Govers) Commit(ctx context.Context, author string, obj any) (*Commit, error) {
	return g.CommitWithProperties(ctx, author, obj, nil)
}

// CommitWithProperties saves a new version of the object with custom commit properties.
func (g *Govers) CommitWithProperties(ctx context.Context, author string, obj any, properties map[string]string) (*Commit, error) {
	if g.repository == nil {
		return nil, fmt.Errorf("repository not configured")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Get the current head commit ID
	headID, err := g.repository.GetHeadID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get head commit ID: %w", err)
	}

	// Create new commit ID
	newCommitID := headID.Next()

	// Create commit metadata
	metadata := NewCommitMetadata(newCommitID, author)
	for k, v := range properties {
		metadata = metadata.WithProperty(k, v)
	}

	// Extract global ID
	globalID, err := g.snapshotFactory.ExtractGlobalID(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to extract global ID: %w", err)
	}

	// Extract current state as SnapshotState
	currentState, err := g.snapshotFactory.ExtractState(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to extract state: %w", err)
	}

	// Get the latest snapshot for this object
	latestSnapshot, err := g.repository.GetLatestSnapshot(ctx, globalID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	// Determine snapshot type and version
	var snapshotType SnapshotType
	var version int64
	var changedProperties []string

	if latestSnapshot == nil {
		// First time committing this object
		snapshotType = Initial
		version = 1
	} else {
		// Update existing object - compare states directly
		snapshotType = Update
		version = latestSnapshot.Version + 1

		// Compare current state with previous state directly
		changedProperties = CompareStates(latestSnapshot.State, currentState)

		// If nothing changed, don't create a new snapshot
		if len(changedProperties) == 0 {
			return nil, nil
		}
	}

	// Create snapshot
	snapshot := NewSnapshot(globalID, currentState, snapshotType, version, metadata).
		WithChangedProperties(changedProperties)

	// Create commit
	commit := NewCommit(metadata).WithSnapshot(snapshot)

	// Create changes based on comparing states
	var previousState SnapshotState
	if latestSnapshot != nil {
		previousState = latestSnapshot.State
	}
	changes := g.createChangesFromStates(globalID, metadata, previousState, currentState, snapshotType, changedProperties)
	for _, change := range changes {
		commit = commit.WithChange(change)
	}

	// Persist the commit
	if err := g.repository.Persist(ctx, commit); err != nil {
		return nil, fmt.Errorf("failed to persist commit: %w", err)
	}

	return &commit, nil
}

// Delete marks an object as deleted by creating a TERMINAL snapshot.
func (g *Govers) Delete(ctx context.Context, author string, obj any) (*Commit, error) {
	if g.repository == nil {
		return nil, fmt.Errorf("repository not configured")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Get the current head commit ID
	headID, err := g.repository.GetHeadID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get head commit ID: %w", err)
	}

	// Create new commit ID
	newCommitID := headID.Next()

	// Create commit metadata
	metadata := NewCommitMetadata(newCommitID, author)

	// Extract global ID
	globalID, err := g.snapshotFactory.ExtractGlobalID(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to extract global ID: %w", err)
	}

	// Get the latest snapshot for this object
	latestSnapshot, err := g.repository.GetLatestSnapshot(ctx, globalID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	if latestSnapshot == nil {
		return nil, fmt.Errorf("cannot delete object that was never committed")
	}

	if latestSnapshot.IsTerminal() {
		return nil, fmt.Errorf("object is already deleted")
	}

	// Create terminal snapshot (empty state)
	version := latestSnapshot.Version + 1
	snapshot := NewSnapshot(globalID, EmptySnapshotState(), Terminal, version, metadata)

	// Create commit
	commit := NewCommit(metadata).
		WithSnapshot(snapshot).
		WithChange(NewObjectRemoved(globalID, metadata))

	// Persist the commit
	if err := g.repository.Persist(ctx, commit); err != nil {
		return nil, fmt.Errorf("failed to persist commit: %w", err)
	}

	return &commit, nil
}

// FindSnapshots returns snapshots matching the given query.
func (g *Govers) FindSnapshots(ctx context.Context, query Query) ([]Snapshot, error) {
	if g.repository == nil {
		return nil, fmt.Errorf("repository not configured")
	}
	return g.repository.GetSnapshots(ctx, query)
}

// GetLatestSnapshot returns the most recent snapshot for an object.
func (g *Govers) GetLatestSnapshot(ctx context.Context, typeName string, id any) (*Snapshot, error) {
	if g.repository == nil {
		return nil, fmt.Errorf("repository not configured")
	}
	globalID := NewInstanceID(typeName, id)
	return g.repository.GetLatestSnapshot(ctx, globalID)
}

// GetSnapshot returns a specific version of an object's snapshot.
func (g *Govers) GetSnapshot(ctx context.Context, typeName string, id any, version int64) (*Snapshot, error) {
	if g.repository == nil {
		return nil, fmt.Errorf("repository not configured")
	}
	globalID := NewInstanceID(typeName, id)
	return g.repository.GetSnapshot(ctx, globalID, version)
}

func (g *Govers) createChangesFromStates(globalID GlobalID, metadata CommitMetadata, previousState, currentState SnapshotState, snapshotType SnapshotType, changedProperties []string) []Change {
	var changes []Change

	if snapshotType == Initial {
		changes = append(changes, NewNewObjectCreated(globalID, metadata))
		return changes
	}

	for _, propName := range changedProperties {
		oldValue := previousState.GetPropertyValue(propName)
		newValue := currentState.GetPropertyValue(propName)

		change := g.createChangeForProperty(globalID, metadata, propName, oldValue, newValue)
		if change != nil {
			changes = append(changes, change)
		}
	}

	return changes
}

// Value type constants for getValueType function.
const (
	valueTypeSlice = "slice"
	valueTypeMap   = "map"
	valueTypeValue = "value"
	valueTypeNil   = "nil"
)

func (g *Govers) createChangeForProperty(globalID GlobalID, metadata CommitMetadata, propName string, oldValue, newValue any) Change {
	oldType := getValueType(oldValue)
	newType := getValueType(newValue)

	if newType == valueTypeSlice || oldType == valueTypeSlice {
		return g.createListChangeFromValues(globalID, metadata, propName, oldValue, newValue)
	}

	if newType == valueTypeMap || oldType == valueTypeMap {
		return g.createMapChangeFromValues(globalID, metadata, propName, oldValue, newValue)
	}

	return NewValueChange(globalID, metadata, propName, oldValue, newValue)
}

func getValueType(v any) string {
	if v == nil {
		return valueTypeNil
	}
	rv := reflect.ValueOf(v)
	//nolint:exhaustive // intentionally using default for all other kinds
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return valueTypeSlice
	case reflect.Map:
		return valueTypeMap
	default:
		return valueTypeValue
	}
}

func (g *Govers) createListChangeFromValues(globalID GlobalID, metadata CommitMetadata, propName string, oldValue, newValue any) ListChange {
	var elementChanges []ElementChange

	oldSlice := toSlice(oldValue)
	newSlice := toSlice(newValue)
	oldMatched := make([]bool, len(oldSlice))

	for i, newElem := range newSlice {
		found := false
		for j, oldElem := range oldSlice {
			if !oldMatched[j] && valuesEqual(oldElem, newElem) {
				oldMatched[j] = true
				found = true
				break
			}
		}
		if !found {
			elementChanges = append(elementChanges, ElementChange{
				Index: i,
				Type:  "ADDED",
				Value: newElem,
			})
		}
	}

	for j, oldElem := range oldSlice {
		if !oldMatched[j] {
			elementChanges = append(elementChanges, ElementChange{
				Index: j,
				Type:  "REMOVED",
				Value: oldElem,
			})
		}
	}

	return NewListChange(globalID, metadata, propName, elementChanges)
}

func toSlice(v any) []any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil
	}
	result := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result
}

func (g *Govers) createMapChangeFromValues(globalID GlobalID, metadata CommitMetadata, propName string, oldValue, newValue any) MapChange {
	var entryChanges []EntryChange

	oldMap := toMap(oldValue)
	newMap := toMap(newValue)

	for key, newVal := range newMap {
		if oldVal, exists := oldMap[key]; exists {
			if !valuesEqual(oldVal, newVal) {
				entryChanges = append(entryChanges, EntryChange{
					Key:   key,
					Type:  "CHANGED",
					Left:  oldVal,
					Right: newVal,
				})
			}
		} else {
			entryChanges = append(entryChanges, EntryChange{
				Key:   key,
				Type:  "ADDED",
				Left:  nil,
				Right: newVal,
			})
		}
	}

	for key, oldVal := range oldMap {
		if _, exists := newMap[key]; !exists {
			entryChanges = append(entryChanges, EntryChange{
				Key:   key,
				Type:  "REMOVED",
				Left:  oldVal,
				Right: nil,
			})
		}
	}

	return NewMapChange(globalID, metadata, propName, entryChanges)
}

func toMap(v any) map[any]any {
	if v == nil {
		return make(map[any]any)
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map {
		return make(map[any]any)
	}
	result := make(map[any]any)
	for _, key := range rv.MapKeys() {
		result[key.Interface()] = rv.MapIndex(key).Interface()
	}
	return result
}
