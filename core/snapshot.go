package core

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Tag constants for struct field annotations.
const (
	// TagName is the struct tag key for govers annotations.
	TagName = "govers"
	// TagID marks a field as the entity ID.
	TagID = "id"
	// TagIgnore marks a field to be ignored.
	TagIgnore = "ignore"
	// TagEntity marks a field as an entity reference.
	TagEntity = "entity"
	// TagIgnoreOrder marks a slice field to ignore element order when comparing.
	TagIgnoreOrder = "ignoreOrder"
)

var (
	// ErrNotStruct is returned when a non-struct type is passed where a struct is expected.
	ErrNotStruct = errors.New("value must be a struct or pointer to struct")
	// ErrNoIDField is returned when an entity has no ID field defined.
	ErrNoIDField = errors.New("entity must have an ID field (use `govers:\"id\"` tag)")
	// ErrNilValue is returned when a nil value is passed.
	ErrNilValue = errors.New("value cannot be nil")
	// ErrInvalidEntityReference is returned when an entity-tagged field is not
	// a struct reference with an ID.
	ErrInvalidEntityReference = errors.New("entity reference must be a struct with an ID field")
)

// SnapshotFactory creates snapshots from domain objects.
type SnapshotFactory struct {
	// TypeNameFunc is used to get the type name for an object.
	// If nil, the reflect type name is used.
	TypeNameFunc func(obj any) string
}

// NewSnapshotFactory creates a new SnapshotFactory with default settings.
func NewSnapshotFactory() *SnapshotFactory {
	return &SnapshotFactory{}
}

// WithTypeNameFunc sets a custom function for determining type names.
func (f *SnapshotFactory) WithTypeNameFunc(fn func(obj any) string) *SnapshotFactory {
	f.TypeNameFunc = fn
	return f
}

// CreateSnapshot creates a snapshot from a domain object.
func (f *SnapshotFactory) CreateSnapshot(obj any, snapshotType SnapshotType, version int64, metadata CommitMetadata) (Snapshot, error) {
	globalID, err := f.ExtractGlobalID(obj)
	if err != nil {
		return Snapshot{}, err
	}

	state, err := f.ExtractState(obj)
	if err != nil {
		return Snapshot{}, err
	}

	return NewSnapshot(globalID, state, snapshotType, version, metadata), nil
}

// ExtractGlobalID extracts the GlobalID from a domain object.
func (f *SnapshotFactory) ExtractGlobalID(obj any) (GlobalID, error) {
	if obj == nil {
		return nil, ErrNilValue
	}

	v := reflect.ValueOf(obj)
	var ok bool
	if v, ok = indirectValue(v); !ok {
		return nil, ErrNilValue
	}

	if v.Kind() != reflect.Struct {
		return nil, ErrNotStruct
	}

	typeName := f.getTypeName(obj, v.Type())
	idValue, err := f.extractIDValue(v)
	if err != nil {
		return nil, err
	}

	return NewInstanceID(typeName, idValue), nil
}

// ExtractState extracts the state as a SnapshotState from a domain object.
// Entity references are "dehydrated" to their GlobalId string representations.
func (f *SnapshotFactory) ExtractState(obj any) (SnapshotState, error) {
	if obj == nil {
		return EmptySnapshotState(), ErrNilValue
	}

	v := reflect.ValueOf(obj)
	var ok bool
	if v, ok = indirectValue(v); !ok {
		return EmptySnapshotState(), ErrNilValue
	}

	if v.Kind() != reflect.Struct {
		return EmptySnapshotState(), ErrNotStruct
	}

	return f.extractStateFromValue(v)
}

func (f *SnapshotFactory) getTypeName(obj any, t reflect.Type) string {
	if f.TypeNameFunc != nil {
		return f.TypeNameFunc(obj)
	}
	return t.Name()
}

func (f *SnapshotFactory) extractIDValue(v reflect.Value) (any, error) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if hasGoversTag(field.Tag.Get(TagName), TagID) {
			fieldValue := v.Field(i)
			if !fieldValue.CanInterface() {
				continue
			}
			return fieldValue.Interface(), nil
		}
	}

	for _, name := range []string{"ID", "Id", "id", "Uuid", "UUID", "uuid"} {
		if field := v.FieldByName(name); field.IsValid() && field.CanInterface() {
			return field.Interface(), nil
		}
	}

	return nil, ErrNoIDField
}

func (f *SnapshotFactory) extractStateFromValue(v reflect.Value) (SnapshotState, error) {
	builder := NewSnapshotStateBuilder()
	seenProperties := make(map[string]struct{})
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		if !fieldValue.CanInterface() {
			continue
		}

		tag := field.Tag.Get(TagName)
		if hasGoversTag(tag, TagIgnore) {
			continue
		}

		fieldName, include := f.getFieldName(field)
		if !include {
			continue
		}
		if _, exists := seenProperties[fieldName]; exists {
			return EmptySnapshotState(), fmt.Errorf("duplicate snapshot property name %q", fieldName)
		}
		seenProperties[fieldName] = struct{}{}

		if hasGoversTag(tag, TagIgnoreOrder) {
			builder.WithIgnoreOrderProperty(fieldName)
		}
		if hasGoversTag(tag, TagEntity) {
			builder.WithEntityProperty(fieldName)
		}

		value, err := f.extractValue(fieldValue, tag)
		if err != nil {
			return EmptySnapshotState(), fmt.Errorf("field %s: %w", field.Name, err)
		}
		builder.WithPropertyValue(fieldName, value)
	}

	return builder.Build(), nil
}

func (f *SnapshotFactory) getFieldName(field reflect.StructField) (string, bool) {
	jsonTag, present := field.Tag.Lookup("json")
	if !present {
		return field.Name, true
	}

	name, _, _ := strings.Cut(jsonTag, ",")
	if name == "-" {
		return "", false
	}
	if name == "" {
		return field.Name, true
	}
	return name, true
}

func (f *SnapshotFactory) extractValue(v reflect.Value, tag string) (any, error) {
	original := v.Interface()
	var ok bool
	if v, ok = indirectValue(v); !ok {
		return nil, nil
	}

	if hasGoversTag(tag, TagEntity) {
		if v.Kind() != reflect.Struct {
			return nil, ErrInvalidEntityReference
		}
		id, err := f.extractIDValue(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidEntityReference, err)
		}
		typeName := f.getTypeName(original, v.Type())
		return NewInstanceID(typeName, id).Value(), nil
	}

	if v.CanInterface() {
		return v.Interface(), nil
	}

	return nil, nil
}

func indirectValue(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}, false
		}
		v = v.Elem()
	}
	return v, v.IsValid()
}

func hasGoversTag(tag, option string) bool {
	for part := range strings.SplitSeq(tag, ",") {
		if strings.TrimSpace(part) == option {
			return true
		}
	}
	return false
}

// CompareStates compares two SnapshotStates and returns the list of changed property names.
// This is the primary comparison method using normalized state representations.
func CompareStates(oldState, newState SnapshotState) []string {
	return newState.DifferentValues(oldState)
}

// StatesEqual returns true if two SnapshotStates have identical property values.
func StatesEqual(oldState, newState SnapshotState) bool {
	return newState.Equals(oldState)
}

// GetTypeName returns the type name of an object using reflection.
func GetTypeName(obj any) string {
	if obj == nil {
		return ""
	}
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

// GetTypeNameWithPackage returns the fully qualified type name including package.
func GetTypeNameWithPackage(obj any) string {
	if obj == nil {
		return ""
	}
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.PkgPath() != "" {
		return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
	}
	return t.Name()
}
