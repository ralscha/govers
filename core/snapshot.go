package core

import (
	"errors"
	"fmt"
	"reflect"
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
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, ErrNilValue
		}
		v = v.Elem()
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
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return EmptySnapshotState(), ErrNilValue
		}
		v = v.Elem()
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
		tag := field.Tag.Get(TagName)
		if tag == TagID {
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
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		if !fieldValue.CanInterface() {
			continue
		}

		tag := field.Tag.Get(TagName)
		if tag == TagIgnore {
			continue
		}

		fieldName := f.getFieldName(field)

		// Check if field has ignoreOrder tag
		if tag == TagIgnoreOrder {
			builder.WithIgnoreOrderProperty(fieldName)
		}

		value := f.extractValue(fieldValue, tag)
		builder.WithPropertyValue(fieldName, value)
	}

	return builder.Build(), nil
}

func (f *SnapshotFactory) getFieldName(field reflect.StructField) string {
	if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
		for i := 0; i < len(jsonTag); i++ {
			if jsonTag[i] == ',' {
				return jsonTag[:i]
			}
		}
		return jsonTag
	}
	return field.Name
}

func (f *SnapshotFactory) extractValue(v reflect.Value, tag string) any {
	if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		return nil
	}

	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if tag == TagEntity && v.Kind() == reflect.Struct {
		if id, err := f.extractIDValue(v); err == nil {
			typeName := v.Type().Name()
			return NewInstanceID(typeName, id).Value()
		}
	}

	if v.CanInterface() {
		return v.Interface()
	}

	return nil
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
	if t.Kind() == reflect.Pointer {
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
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.PkgPath() != "" {
		return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
	}
	return t.Name()
}
