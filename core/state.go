package core

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math/big"
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
	entityProperties      []string
}

// NewSnapshotState creates a new SnapshotState from a property map.
func NewSnapshotState(properties map[string]any) SnapshotState {
	return NewSnapshotStateWithOptions(properties, nil)
}

// NewSnapshotStateWithOptions creates a new SnapshotState with property map and ignore order settings.
func NewSnapshotStateWithOptions(properties map[string]any, ignoreOrderProperties []string) SnapshotState {
	return NewSnapshotStateWithMetadata(properties, ignoreOrderProperties, nil)
}

// NewSnapshotStateWithMetadata creates a state with comparison and entity-reference metadata.
func NewSnapshotStateWithMetadata(properties map[string]any, ignoreOrderProperties, entityProperties []string) SnapshotState {
	properties = cloneProperties(properties)
	if ignoreOrderProperties == nil {
		ignoreOrderProperties = []string{}
	} else {
		ignoreOrderProperties = append([]string(nil), ignoreOrderProperties...)
	}
	if entityProperties == nil {
		entityProperties = []string{}
	} else {
		entityProperties = append([]string(nil), entityProperties...)
	}
	return SnapshotState{
		properties:            properties,
		ignoreOrderProperties: ignoreOrderProperties,
		entityProperties:      entityProperties,
	}
}

// Clone returns an independent copy of the state. Collection and pointer
// values are copied recursively so a domain object can be mutated after a
// commit without changing its historical snapshot.
func (s SnapshotState) Clone() SnapshotState {
	return NewSnapshotStateWithMetadata(s.properties, s.ignoreOrderProperties, s.entityProperties)
}

func cloneProperties(properties map[string]any) map[string]any {
	cloned := make(map[string]any, len(properties))
	visited := make(map[cloneVisit]reflect.Value)
	for name, value := range properties {
		if value == nil {
			cloned[name] = nil
			continue
		}
		cloned[name] = cloneReflectValue(reflect.ValueOf(value), visited).Interface()
	}
	return cloned
}

type cloneVisit struct {
	typ reflect.Type
	ptr uintptr
}

func cloneReflectValue(value reflect.Value, visited map[cloneVisit]reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	//nolint:exhaustive // Kinds without reference-containing values are immutable and returned as-is.
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneReflectValue(value.Elem(), visited)
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := cloneVisit{typ: value.Type(), ptr: value.Pointer()}
		if cloned, ok := visited[visit]; ok {
			return cloned
		}
		result := reflect.New(value.Type().Elem())
		visited[visit] = result
		result.Elem().Set(cloneReflectValue(value.Elem(), visited))
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := cloneVisit{typ: value.Type(), ptr: value.Pointer()}
		if cloned, ok := visited[visit]; ok {
			return cloned
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		visited[visit] = result
		iter := value.MapRange()
		for iter.Next() {
			result.SetMapIndex(cloneReflectValue(iter.Key(), visited), cloneReflectValue(iter.Value(), visited))
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := cloneVisit{typ: value.Type(), ptr: value.Pointer()}
		if cloned, ok := visited[visit]; ok {
			return cloned
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		visited[visit] = result
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneReflectValue(value.Index(i), visited))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneReflectValue(value.Index(i), visited))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if result.Field(i).CanSet() && value.Field(i).CanInterface() {
				result.Field(i).Set(cloneReflectValue(value.Field(i), visited))
			}
		}
		return result
	default:
		return value
	}
}

// EmptySnapshotState creates an empty SnapshotState.
func EmptySnapshotState() SnapshotState {
	return SnapshotState{properties: make(map[string]any), ignoreOrderProperties: []string{}, entityProperties: []string{}}
}

// ShouldIgnoreOrder returns true if the property should ignore order when comparing slices.
func (s SnapshotState) ShouldIgnoreOrder(propertyName string) bool {
	return slices.Contains(s.ignoreOrderProperties, propertyName)
}

// GetIgnoreOrderPropertyNames returns the properties with order-insensitive collection comparison.
func (s SnapshotState) GetIgnoreOrderPropertyNames() []string {
	return append([]string(nil), s.ignoreOrderProperties...)
}

// IsEntityReference returns true if the property stores a dehydrated entity reference.
func (s SnapshotState) IsEntityReference(propertyName string) bool {
	return slices.Contains(s.entityProperties, propertyName)
}

// GetEntityPropertyNames returns the properties containing dehydrated entity references.
func (s SnapshotState) GetEntityPropertyNames() []string {
	return append([]string(nil), s.entityProperties...)
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
		if other.IsNull(name) {
			return false
		}
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
	aNumber := numericRat(a)
	bNumber := numericRat(b)
	if aNumber == nil || bNumber == nil {
		// big.Rat cannot represent infinities or NaN. The ordinary floating
		// point comparison gives the expected result for those values.
		if isFloat(a.Kind()) && isFloat(b.Kind()) {
			return a.Float() == b.Float()
		}
		return false
	}
	return aNumber.Cmp(bNumber) == 0
}

func numericRat(v reflect.Value) *big.Rat {
	//nolint:exhaustive // intentionally only handling numeric types
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(big.Rat).SetInt64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		integer := new(big.Int).SetUint64(v.Uint())
		return new(big.Rat).SetInt(integer)
	case reflect.Float32, reflect.Float64:
		return new(big.Rat).SetFloat64(v.Float())
	default:
		return nil
	}
}

func isFloat(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
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

	bKeys := b.MapKeys()
	matched := make([]bool, len(bKeys))
	for _, aKey := range a.MapKeys() {
		found := false
		for i, bKey := range bKeys {
			if matched[i] || !valuesEqual(aKey.Interface(), bKey.Interface()) {
				continue
			}
			if !valuesEqual(a.MapIndex(aKey).Interface(), b.MapIndex(bKey).Interface()) {
				return false
			}
			matched[i] = true
			found = true
			break
		}
		if !found {
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

// snapshotStateJSON is the JSON representation of SnapshotState.
type snapshotStateJSON struct {
	Properties            map[string]any `json:"properties"`
	IgnoreOrderProperties []string       `json:"ignoreOrderProperties,omitempty"`
	EntityProperties      []string       `json:"entityProperties,omitempty"`
}

// MarshalJSON implements the legacy JSON marshaling interface for compatibility.
func (s SnapshotState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.jsonPayload())
}

// MarshalJSONTo implements json.MarshalerTo.
func (s SnapshotState) MarshalJSONTo(out *jsontext.Encoder) error {
	return json.MarshalEncode(out, s.jsonPayload())
}

// UnmarshalJSON implements the legacy JSON unmarshaling interface for compatibility.
func (s *SnapshotState) UnmarshalJSON(data []byte) error {
	var payload snapshotStateJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	return s.setJSONPayload(payload)
}

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (s *SnapshotState) UnmarshalJSONFrom(in *jsontext.Decoder) error {
	var payload snapshotStateJSON
	if err := json.UnmarshalDecode(in, &payload); err != nil {
		return err
	}
	return s.setJSONPayload(payload)
}

func (s SnapshotState) jsonPayload() snapshotStateJSON {
	return snapshotStateJSON{
		Properties:            s.properties,
		IgnoreOrderProperties: s.ignoreOrderProperties,
		EntityProperties:      s.entityProperties,
	}
}

func (s *SnapshotState) setJSONPayload(payload snapshotStateJSON) error {
	if payload.Properties == nil {
		return fmt.Errorf("snapshot state json missing properties")
	}
	s.properties = payload.Properties
	s.ignoreOrderProperties = append([]string(nil), payload.IgnoreOrderProperties...)
	s.entityProperties = append([]string(nil), payload.EntityProperties...)
	return nil
}

var (
	_ json.MarshalerTo     = SnapshotState{}
	_ json.UnmarshalerFrom = (*SnapshotState)(nil)
)

// String returns a string representation of the state.
func (s SnapshotState) String() string {
	names := s.GetPropertyNames()
	var result strings.Builder
	result.WriteString("{")
	for i, name := range names {
		if i > 0 {
			result.WriteString(", ")
		}
		fmt.Fprintf(&result, "%s:%v", name, s.properties[name])
	}
	result.WriteString("}")
	return result.String()
}

// SnapshotStateBuilder helps construct a SnapshotState incrementally.
type SnapshotStateBuilder struct {
	properties            map[string]any
	ignoreOrderProperties []string
	entityProperties      []string
}

// NewSnapshotStateBuilder creates a new builder for SnapshotState.
func NewSnapshotStateBuilder() *SnapshotStateBuilder {
	return &SnapshotStateBuilder{
		properties:            make(map[string]any),
		ignoreOrderProperties: []string{},
		entityProperties:      []string{},
	}
}

// WithEntityProperty marks a property as a dehydrated entity reference.
func (b *SnapshotStateBuilder) WithEntityProperty(propertyName string) *SnapshotStateBuilder {
	if !slices.Contains(b.entityProperties, propertyName) {
		b.entityProperties = append(b.entityProperties, propertyName)
	}
	return b
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
	return NewSnapshotStateWithMetadata(b.properties, b.ignoreOrderProperties, b.entityProperties)
}
