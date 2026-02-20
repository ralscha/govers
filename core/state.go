package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
)

// SnapshotState represents the normalized state of a domain object as a property-value map.
// Similar to JaVers' CdoSnapshotState, entity references are "dehydrated" to GlobalId values,
// allowing direct property-by-property comparison without reconstructing full objects.
type SnapshotState struct {
	properties            map[string]any
	ignoreOrderProperties []string
}

// NewSnapshotState creates a new SnapshotState from a property map.
func NewSnapshotState(properties map[string]any) SnapshotState {
	return NewSnapshotStateWithOptions(properties, nil)
}

// NewSnapshotStateWithOptions creates a new SnapshotState with property map and ignore order settings.
func NewSnapshotStateWithOptions(properties map[string]any, ignoreOrderProperties []string) SnapshotState {
	if properties == nil {
		properties = make(map[string]any)
	}
	if ignoreOrderProperties == nil {
		ignoreOrderProperties = []string{}
	} else {
		ignoreOrderProperties = append([]string(nil), ignoreOrderProperties...)
	}
	return SnapshotState{properties: properties, ignoreOrderProperties: ignoreOrderProperties}
}

// EmptySnapshotState creates an empty SnapshotState.
func EmptySnapshotState() SnapshotState {
	return SnapshotState{properties: make(map[string]any), ignoreOrderProperties: []string{}}
}

// ShouldIgnoreOrder returns true if the property should ignore order when comparing slices.
func (s SnapshotState) ShouldIgnoreOrder(propertyName string) bool {
	return slices.Contains(s.ignoreOrderProperties, propertyName)
}

// Size returns the number of properties in the state.
func (s SnapshotState) Size() int {
	return len(s.properties)
}

// GetPropertyValue returns the value of a property by name.
// Returns nil if the property doesn't exist.
func (s SnapshotState) GetPropertyValue(propertyName string) any {
	return s.properties[propertyName]
}

// IsNull returns true if the property doesn't exist in the state.
func (s SnapshotState) IsNull(propertyName string) bool {
	_, exists := s.properties[propertyName]
	return !exists
}

// GetPropertyNames returns all property names in the state.
func (s SnapshotState) GetPropertyNames() []string {
	names := make([]string, 0, len(s.properties))
	for name := range s.properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ForEachProperty iterates over all properties in the state.
func (s SnapshotState) ForEachProperty(fn func(name string, value any)) {
	for name, value := range s.properties {
		fn(name, value)
	}
}

// Equals compares this state with another state for equality.
// Two states are equal if they have the same properties with equal values.
func (s SnapshotState) Equals(other SnapshotState) bool {
	if len(s.properties) != len(other.properties) {
		return false
	}

	for name := range s.properties {
		if !s.propertyEquals(other, name) {
			return false
		}
	}

	return true
}

func (s SnapshotState) propertyEquals(other SnapshotState, propertyName string) bool {
	thisValue := s.GetPropertyValue(propertyName)
	otherValue := other.GetPropertyValue(propertyName)
	ignoreOrder := s.ShouldIgnoreOrder(propertyName) || other.ShouldIgnoreOrder(propertyName)

	return valuesEqualWithOptions(thisValue, otherValue, ignoreOrder)
}

func valuesEqual(a, b any) bool {
	return valuesEqualWithOptions(a, b, false)
}

func valuesEqualWithOptions(a, b any, ignoreOrder bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if aGid, ok := a.(GlobalID); ok {
		if bGid, ok := b.(GlobalID); ok {
			return aGid.Value() == bGid.Value()
		}
		return false
	}

	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	if isNumeric(aVal.Kind()) && isNumeric(bVal.Kind()) {
		return numericEqual(aVal, bVal)
	}

	if aVal.Type() != bVal.Type() {
		// Allow comparison between different slice types (e.g. []string vs []interface{})
		// and different map types
		isSliceA := aVal.Kind() == reflect.Slice || aVal.Kind() == reflect.Array
		isSliceB := bVal.Kind() == reflect.Slice || bVal.Kind() == reflect.Array
		isMapA := aVal.Kind() == reflect.Map
		isMapB := bVal.Kind() == reflect.Map

		bothSlices := isSliceA && isSliceB
		bothMaps := isMapA && isMapB
		if !bothSlices && !bothMaps {
			return false
		}
	}

	//nolint:exhaustive // intentionally using default for all other kinds
	switch aVal.Kind() {
	case reflect.Slice, reflect.Array:
		if ignoreOrder {
			return slicesEqualIgnoreOrder(aVal, bVal)
		}
		return slicesEqual(aVal, bVal)
	case reflect.Map:
		return mapsEqual(aVal, bVal)
	case reflect.Struct:
		return structsEqual(aVal, bVal)
	default:
		return reflect.DeepEqual(a, b)
	}
}

func isNumeric(k reflect.Kind) bool {
	//nolint:exhaustive // intentionally only handling numeric types
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func numericEqual(a, b reflect.Value) bool {
	aFloat := toFloat64(a)
	bFloat := toFloat64(b)
	return aFloat == bFloat
}

func toFloat64(v reflect.Value) float64 {
	//nolint:exhaustive // intentionally only handling numeric types
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		return 0
	}
}

func slicesEqual(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		if !valuesEqual(a.Index(i).Interface(), b.Index(i).Interface()) {
			return false
		}
	}
	return true
}

func slicesEqualIgnoreOrder(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}

	if a.Len() == 0 {
		return true
	}

	// Optimization for simple comparable types (strings, ints, bools)
	// This avoids O(N^2) complexity for common cases
	elemType := a.Type().Elem()
	if elemType == b.Type().Elem() && isSimpleComparable(elemType.Kind()) {
		counts := make(map[any]int, a.Len())
		for i := 0; i < a.Len(); i++ {
			val := a.Index(i).Interface()
			counts[val]++
		}
		for i := 0; i < b.Len(); i++ {
			val := b.Index(i).Interface()
			count, ok := counts[val]
			if !ok || count == 0 {
				return false
			}
			counts[val]--
		}
		return true
	}

	matched := make([]bool, b.Len())

	for i := 0; i < a.Len(); i++ {
		aElem := a.Index(i).Interface()
		found := false

		for j := 0; j < b.Len(); j++ {
			if matched[j] {
				continue
			}
			bElem := b.Index(j).Interface()
			if valuesEqual(aElem, bElem) {
				matched[j] = true
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func isSimpleComparable(k reflect.Kind) bool {
	//nolint:exhaustive // intentionally only handling simple comparable types
	switch k {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Bool:
		return true
	default:
		return false
	}
}

func mapsEqual(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for _, key := range a.MapKeys() {
		aVal := a.MapIndex(key)
		bVal := b.MapIndex(key)
		if !bVal.IsValid() {
			return false
		}
		if !valuesEqual(aVal.Interface(), bVal.Interface()) {
			return false
		}
	}
	return true
}

func structsEqual(a, b reflect.Value) bool {
	return reflect.DeepEqual(a.Interface(), b.Interface())
}

// DifferentValues returns a list of property names that have different values
// compared to the previous state. This includes:
// - Properties with changed values
// - Properties that were added (exist in current but not in previous)
// - Properties that were removed (exist in previous but not in current)
func (s SnapshotState) DifferentValues(previous SnapshotState) []string {
	differentSet := make(map[string]struct{})

	// Check for changed values in current state
	for propertyName := range s.properties {
		if previous.IsNull(propertyName) {
			// Property was added
			differentSet[propertyName] = struct{}{}
			continue
		}
		if !s.propertyEquals(previous, propertyName) {
			differentSet[propertyName] = struct{}{}
		}
	}

	// Check for removed properties (exist in previous but not in current)
	for propertyName := range previous.properties {
		if s.IsNull(propertyName) {
			differentSet[propertyName] = struct{}{}
		}
	}

	different := make([]string, 0, len(differentSet))
	for name := range differentSet {
		different = append(different, name)
	}
	sort.Strings(different)
	return different
}

// MarshalJSON implements json.Marshaler for SnapshotState.
type snapshotStateJSON struct {
	Properties            map[string]any `json:"properties"`
	IgnoreOrderProperties []string       `json:"ignoreOrderProperties,omitempty"`
}

// MarshalJSON implements json.Marshaler for SnapshotState.
func (s SnapshotState) MarshalJSON() ([]byte, error) {
	payload := snapshotStateJSON{
		Properties:            s.properties,
		IgnoreOrderProperties: s.ignoreOrderProperties,
	}
	return json.Marshal(payload)
}

// UnmarshalJSON implements json.Unmarshaler for SnapshotState.
func (s *SnapshotState) UnmarshalJSON(data []byte) error {
	var payload snapshotStateJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if payload.Properties == nil {
		return fmt.Errorf("snapshot state json missing properties")
	}
	s.properties = payload.Properties
	s.ignoreOrderProperties = append([]string(nil), payload.IgnoreOrderProperties...)
	return nil
}

// String returns a string representation of the state.
func (s SnapshotState) String() string {
	names := s.GetPropertyNames()
	var result strings.Builder
	result.WriteString("{")
	for i, name := range names {
		if i > 0 {
			result.WriteString(", ")
		}
		result.WriteString(fmt.Sprintf("%s:%v", name, s.properties[name]))
	}
	result.WriteString("}")
	return result.String()
}

// SnapshotStateBuilder helps construct a SnapshotState incrementally.
type SnapshotStateBuilder struct {
	properties            map[string]any
	ignoreOrderProperties []string
}

// NewSnapshotStateBuilder creates a new builder for SnapshotState.
func NewSnapshotStateBuilder() *SnapshotStateBuilder {
	return &SnapshotStateBuilder{
		properties:            make(map[string]any),
		ignoreOrderProperties: []string{},
	}
}

// WithIgnoreOrderProperty marks a property to ignore element order when comparing slices.
func (b *SnapshotStateBuilder) WithIgnoreOrderProperty(propertyName string) *SnapshotStateBuilder {
	if slices.Contains(b.ignoreOrderProperties, propertyName) {
		return b
	}
	b.ignoreOrderProperties = append(b.ignoreOrderProperties, propertyName)
	return b
}

// WithPropertyValue adds a property to the state being built.
// Nil values are skipped.
func (b *SnapshotStateBuilder) WithPropertyValue(propertyName string, value any) *SnapshotStateBuilder {
	if value == nil {
		return b
	}
	b.properties[propertyName] = value
	return b
}

// Contains returns true if the builder already has a value for the given property.
func (b *SnapshotStateBuilder) Contains(propertyName string) bool {
	_, exists := b.properties[propertyName]
	return exists
}

// Build creates the SnapshotState from the builder.
func (b *SnapshotStateBuilder) Build() SnapshotState {
	return NewSnapshotStateWithOptions(b.properties, b.ignoreOrderProperties)
}
