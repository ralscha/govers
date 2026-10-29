package core

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
)

var (
	// ErrRepositoryNotConfigured is returned when an operation requires a repository.
	ErrRepositoryNotConfigured = errors.New("repository not configured")
	// ErrObjectDeleted is returned when attempting to commit a terminal object.
	ErrObjectDeleted = errors.New("object is deleted")
	// ErrInvalidVersion is returned when a snapshot version is not positive.
	ErrInvalidVersion = errors.New("snapshot version must be positive")
	// ErrConcurrentCommit is returned when another writer advances repository
	// history after a commit has selected its ID.
	ErrConcurrentCommit = errors.New("concurrent commit conflict")
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
		if opt != nil {
			opt(g)
		}
	}
	if g.snapshotFactory == nil {
		g.snapshotFactory = NewSnapshotFactory()
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
	if !repositoryAvailable(g.repository) {
		return nil, ErrRepositoryNotConfigured
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
	metadata := NewCommitMetadata(newCommitID, author).WithProperties(properties)

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
		if latestSnapshot.IsTerminal() {
			return nil, ErrObjectDeleted
		}
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
	commit.Changes = g.createChangesFromStates(globalID, metadata, previousState, currentState, snapshotType, changedProperties)

	// Persist the commit
	if err := g.repository.Persist(ctx, commit); err != nil {
		return nil, fmt.Errorf("failed to persist commit: %w", err)
	}

	return &commit, nil
}

// Delete marks an object as deleted by creating a TERMINAL snapshot.
func (g *Govers) Delete(ctx context.Context, author string, obj any) (*Commit, error) {
	return g.DeleteWithProperties(ctx, author, obj, nil)
}

// DeleteWithProperties marks an object as deleted and attaches custom commit properties.
func (g *Govers) DeleteWithProperties(ctx context.Context, author string, obj any, properties map[string]string) (*Commit, error) {
	if !repositoryAvailable(g.repository) {
		return nil, ErrRepositoryNotConfigured
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
	metadata := NewCommitMetadata(newCommitID, author).WithProperties(properties)

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
		return nil, fmt.Errorf("object is already deleted: %w", ErrObjectDeleted)
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
	if !repositoryAvailable(g.repository) {
		return nil, ErrRepositoryNotConfigured
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	return g.repository.GetSnapshots(ctx, query)
}

// GetLatestSnapshot returns the most recent snapshot for an object.
func (g *Govers) GetLatestSnapshot(ctx context.Context, typeName string, id any) (*Snapshot, error) {
	if !repositoryAvailable(g.repository) {
		return nil, ErrRepositoryNotConfigured
	}
	globalID := NewInstanceID(typeName, id)
	return g.repository.GetLatestSnapshot(ctx, globalID)
}

// GetSnapshot returns a specific version of an object's snapshot.
func (g *Govers) GetSnapshot(ctx context.Context, typeName string, id any, version int64) (*Snapshot, error) {
	if !repositoryAvailable(g.repository) {
		return nil, ErrRepositoryNotConfigured
	}
	if version <= 0 {
		return nil, ErrInvalidVersion
	}
	globalID := NewInstanceID(typeName, id)
	return g.repository.GetSnapshot(ctx, globalID, version)
}

func repositoryAvailable(repository Repository) bool {
	if repository == nil {
		return false
	}
	value := reflect.ValueOf(repository)
	//nolint:exhaustive // Only kinds that can contain a typed nil need special handling.
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return true
	}
}

func (g *Govers) createChangesFromStates(globalID GlobalID, metadata CommitMetadata, previousState, currentState SnapshotState, snapshotType SnapshotType, changedProperties []string) []Change {
	if snapshotType == Initial {
		return []Change{NewNewObjectCreated(globalID, metadata)}
	}

	changes := make([]Change, 0, len(changedProperties))
	for _, propName := range changedProperties {
		oldValue := previousState.GetPropertyValue(propName)
		newValue := currentState.GetPropertyValue(propName)

		ignoreOrder := previousState.ShouldIgnoreOrder(propName) || currentState.ShouldIgnoreOrder(propName)
		entityReference := previousState.IsEntityReference(propName) || currentState.IsEntityReference(propName)
		changes = append(changes, g.createChangeForProperty(globalID, metadata, propName, oldValue, newValue, ignoreOrder, entityReference))
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

func (g *Govers) createChangeForProperty(globalID GlobalID, metadata CommitMetadata, propName string, oldValue, newValue any, ignoreOrder, entityReference bool) Change {
	if entityReference {
		return NewReferenceChange(globalID, metadata, propName, dehydratedInstanceID(oldValue), dehydratedInstanceID(newValue))
	}
	oldType := getValueType(oldValue)
	newType := getValueType(newValue)

	if newType == valueTypeSlice || oldType == valueTypeSlice {
		return g.createListChangeFromValues(globalID, metadata, propName, oldValue, newValue, ignoreOrder)
	}

	if newType == valueTypeMap || oldType == valueTypeMap {
		return g.createMapChangeFromValues(globalID, metadata, propName, oldValue, newValue)
	}

	return NewValueChange(globalID, metadata, propName, oldValue, newValue)
}

func dehydratedInstanceID(value any) GlobalID {
	reference, ok := value.(string)
	if !ok {
		return nil
	}
	typeName, localID, ok := strings.Cut(reference, "/")
	if !ok {
		return nil
	}
	return NewInstanceID(typeName, localID)
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

func (g *Govers) createListChangeFromValues(globalID GlobalID, metadata CommitMetadata, propName string, oldValue, newValue any, ignoreOrder bool) ListChange {
	var elementChanges []ElementChange

	oldSlice := toSlice(oldValue)
	newSlice := toSlice(newValue)
	if !ignoreOrder {
		prefixLength := 0
		for prefixLength < min(len(oldSlice), len(newSlice)) && valuesEqual(oldSlice[prefixLength], newSlice[prefixLength]) {
			prefixLength++
		}
		suffixLength := 0
		for suffixLength < min(len(oldSlice)-prefixLength, len(newSlice)-prefixLength) &&
			valuesEqual(oldSlice[len(oldSlice)-1-suffixLength], newSlice[len(newSlice)-1-suffixLength]) {
			suffixLength++
		}

		oldMiddle := oldSlice[prefixLength : len(oldSlice)-suffixLength]
		newMiddle := newSlice[prefixLength : len(newSlice)-suffixLength]
		commonLength := min(len(oldMiddle), len(newMiddle))
		for i := range commonLength {
			if !valuesEqual(oldMiddle[i], newMiddle[i]) {
				elementChanges = append(elementChanges, ElementChange{Index: prefixLength + i, Type: ChangedChangeType, Value: newMiddle[i]})
			}
		}
		for i := commonLength; i < len(newMiddle); i++ {
			elementChanges = append(elementChanges, ElementChange{Index: prefixLength + i, Type: AddedChangeType, Value: newMiddle[i]})
		}
		for i := commonLength; i < len(oldMiddle); i++ {
			elementChanges = append(elementChanges, ElementChange{Index: prefixLength + i, Type: RemovedChangeType, Value: oldMiddle[i]})
		}
		return NewListChange(globalID, metadata, propName, elementChanges)
	}

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
				Type:  AddedChangeType,
				Value: newElem,
			})
		}
	}

	for j, oldElem := range oldSlice {
		if !oldMatched[j] {
			elementChanges = append(elementChanges, ElementChange{
				Index: j,
				Type:  RemovedChangeType,
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

	oldEntries := sortedMapEntries(oldValue)
	newEntries := sortedMapEntries(newValue)
	matched := make([]bool, len(oldEntries))

	// Ordinary string/integer-keyed maps can match entries in constant time.
	// Heterogeneous keys use the same comparison as snapshot state (for
	// example, int32(1) and int64(1) identify the same entry).
	var keyIndex map[any]int
	var normalizedKeys []string
	if oldType, newType := reflect.TypeOf(oldValue), reflect.TypeOf(newValue); oldType != nil && newType != nil &&
		oldType.Kind() == reflect.Map && newType.Kind() == reflect.Map {
		if oldType.Key() == newType.Key() && isSimpleComparable(oldType.Key().Kind()) {
			keyIndex = make(map[any]int, len(oldEntries))
			for i, entry := range oldEntries {
				keyIndex[entry.key] = i
			}
		} else if oldType.Key() != newType.Key() && (oldType.Key().Kind() == reflect.String || newType.Key().Kind() == reflect.String) {
			// JSON object keys are strings, including integer and TextMarshaler
			// keys. Match their stored names while retaining the original keys
			// in the reported changes.
			oldKeys := jsonMapEntryKeys(oldEntries)
			newKeys := jsonMapEntryKeys(newEntries)
			if oldKeys != nil && newKeys != nil {
				keyIndex = make(map[any]int, len(oldKeys))
				for i, key := range oldKeys {
					keyIndex[key] = i
				}
				normalizedKeys = newKeys
			}
		}
	}

	for entryIndex, entry := range newEntries {
		oldIndex := -1
		if keyIndex != nil {
			key := entry.key
			if normalizedKeys != nil {
				key = normalizedKeys[entryIndex]
			}
			if i, exists := keyIndex[key]; exists {
				oldIndex = i
			}
		} else {
			for i, oldEntry := range oldEntries {
				if !matched[i] && valuesEqual(oldEntry.key, entry.key) {
					oldIndex = i
					break
				}
			}
		}
		if oldIndex >= 0 {
			matched[oldIndex] = true
			oldVal := oldEntries[oldIndex].value
			if !valuesEqual(oldVal, entry.value) {
				entryChanges = append(entryChanges, EntryChange{
					Key:   entry.key,
					Type:  ChangedChangeType,
					Left:  oldVal,
					Right: entry.value,
				})
			}
		} else {
			entryChanges = append(entryChanges, EntryChange{
				Key:   entry.key,
				Type:  AddedChangeType,
				Left:  nil,
				Right: entry.value,
			})
		}
	}

	for i, entry := range oldEntries {
		if !matched[i] {
			entryChanges = append(entryChanges, EntryChange{
				Key:   entry.key,
				Type:  RemovedChangeType,
				Left:  entry.value,
				Right: nil,
			})
		}
	}

	return NewMapChange(globalID, metadata, propName, entryChanges)
}

type mapEntry struct {
	key     any
	value   any
	sortKey string
}

func jsonMapEntryKeys(entries []mapEntry) []string {
	indexes := make(map[any]int, len(entries))
	for i, entry := range entries {
		indexes[entry.key] = i
	}
	data, err := json.Marshal(indexes)
	if err != nil {
		return nil
	}
	var normalized map[string]int
	if err := json.Unmarshal(data, &normalized); err != nil {
		return nil
	}
	keys := make([]string, len(entries))
	for key, index := range normalized {
		keys[index] = key
	}
	return keys
}

func sortedMapEntries(v any) []mapEntry {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map {
		return nil
	}
	entries := make([]mapEntry, 0, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key().Interface()
		entries = append(entries, mapEntry{key: key, value: iter.Value().Interface(), sortKey: fmt.Sprintf("%T:%#v", key, key)})
	}
	slices.SortFunc(entries, func(a, b mapEntry) int { return strings.Compare(a.sortKey, b.sortKey) })
	return entries
}
