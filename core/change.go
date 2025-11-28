package core

// ChangeType indicates the kind of change detected.
type ChangeType string

const (
	// NewObjectChange indicates a new object was committed for the first time.
	NewObjectChange ChangeType = "NewObject"
	// ObjectRemovedChange indicates an object was deleted.
	ObjectRemovedChange ChangeType = "ObjectRemoved"
	// ValueChangeType indicates a primitive or value property changed.
	ValueChangeType ChangeType = "ValueChange"
	// ReferenceChangeType indicates an entity reference changed.
	ReferenceChangeType ChangeType = "ReferenceChange"
	// ListChangeType indicates a list/slice property changed.
	ListChangeType ChangeType = "ListChange"
	// MapChangeType indicates a map property changed.
	MapChangeType ChangeType = "MapChange"
)

// Change represents a difference between two versions of an object.
type Change interface {
	// AffectedGlobalID returns the GlobalID of the object that changed.
	AffectedGlobalID() GlobalID

	// GetCommitMetadata returns the commit metadata associated with this change.
	GetCommitMetadata() CommitMetadata

	// GetChangeType returns the type of this change.
	GetChangeType() ChangeType

	// isChange is a marker method to prevent external implementations.
	isChange()
}

type baseChange struct {
	globalID       GlobalID
	commitMetadata CommitMetadata
}

func (c baseChange) AffectedGlobalID() GlobalID {
	return c.globalID
}

func (c baseChange) GetCommitMetadata() CommitMetadata {
	return c.commitMetadata
}

// NewObjectCreated represents when an object is committed for the first time.
type NewObjectCreated struct {
	baseChange
}

// GetChangeType returns the type of this change.
func (c NewObjectCreated) GetChangeType() ChangeType {
	return NewObjectChange
}

func (c NewObjectCreated) isChange() {}

// NewNewObjectCreated creates a NewObjectCreated change.
func NewNewObjectCreated(globalID GlobalID, metadata CommitMetadata) NewObjectCreated {
	return NewObjectCreated{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
	}
}

// ObjectRemoved represents when an object is deleted.
type ObjectRemoved struct {
	baseChange
}

// GetChangeType returns the type of this change.
func (c ObjectRemoved) GetChangeType() ChangeType {
	return ObjectRemovedChange
}

func (c ObjectRemoved) isChange() {}

// NewObjectRemoved creates an ObjectRemoved change.
func NewObjectRemoved(globalID GlobalID, metadata CommitMetadata) ObjectRemoved {
	return ObjectRemoved{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
	}
}

// ValueChange represents when a primitive or value property changed.
type ValueChange struct {
	baseChange
	PropertyName string
	Left         any // Previous value (nil for new properties)
	Right        any // New value (nil for removed properties)
}

// GetChangeType returns the type of this change.
func (c ValueChange) GetChangeType() ChangeType {
	return ValueChangeType
}

func (c ValueChange) isChange() {}

// NewValueChange creates a ValueChange.
func NewValueChange(globalID GlobalID, metadata CommitMetadata, propertyName string, left, right any) ValueChange {
	return ValueChange{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
		PropertyName: propertyName,
		Left:         left,
		Right:        right,
	}
}

// ReferenceChange represents when an entity reference property changed.
type ReferenceChange struct {
	baseChange
	PropertyName string
	Left         GlobalID // Previous reference (nil for new references)
	Right        GlobalID // New reference (nil for removed references)
}

// GetChangeType returns the type of this change.
func (c ReferenceChange) GetChangeType() ChangeType {
	return ReferenceChangeType
}

func (c ReferenceChange) isChange() {}

// NewReferenceChange creates a ReferenceChange.
func NewReferenceChange(globalID GlobalID, metadata CommitMetadata, propertyName string, left, right GlobalID) ReferenceChange {
	return ReferenceChange{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
		PropertyName: propertyName,
		Left:         left,
		Right:        right,
	}
}

// ElementChange represents a change to an element within a collection.
type ElementChange struct {
	Index int
	Type  ChangeType // "ADDED", "REMOVED", "CHANGED"
	Value any
}

// ListChange represents when a list/slice property changed.
type ListChange struct {
	baseChange
	PropertyName   string
	ElementChanges []ElementChange
}

// GetChangeType returns the type of this change.
func (c ListChange) GetChangeType() ChangeType {
	return ListChangeType
}

func (c ListChange) isChange() {}

// NewListChange creates a ListChange.
func NewListChange(globalID GlobalID, metadata CommitMetadata, propertyName string, elementChanges []ElementChange) ListChange {
	return ListChange{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
		PropertyName:   propertyName,
		ElementChanges: elementChanges,
	}
}

// EntryChange represents a change to an entry within a map.
type EntryChange struct {
	Key   any
	Type  ChangeType // "ADDED", "REMOVED", "CHANGED"
	Left  any        // Previous value
	Right any        // New value
}

// MapChange represents when a map property changed.
type MapChange struct {
	baseChange
	PropertyName string
	EntryChanges []EntryChange
}

// GetChangeType returns the type of this change.
func (c MapChange) GetChangeType() ChangeType {
	return MapChangeType
}

func (c MapChange) isChange() {}

// NewMapChange creates a MapChange.
func NewMapChange(globalID GlobalID, metadata CommitMetadata, propertyName string, entryChanges []EntryChange) MapChange {
	return MapChange{
		baseChange: baseChange{
			globalID:       globalID,
			commitMetadata: metadata,
		},
		PropertyName: propertyName,
		EntryChanges: entryChanges,
	}
}

// Ensure types implement Change
var (
	_ Change = NewObjectCreated{}
	_ Change = ObjectRemoved{}
	_ Change = ValueChange{}
	_ Change = ReferenceChange{}
	_ Change = ListChange{}
	_ Change = MapChange{}
)
