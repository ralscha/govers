package core

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testPersonName = "John Doe"

type Address struct {
	ID     int    `govers:"id"`
	Street string `json:"street"`
	City   string `json:"city"`
}

func TestJSONFieldNameOptionsAndIgnoredField(t *testing.T) {
	type document struct {
		ID      int    `govers:"id"`
		Name    string `json:",omitempty"`
		Ignored string `json:"-"`
	}

	state, err := NewSnapshotFactory().ExtractState(document{ID: 1, Name: "Alice", Ignored: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if got := state.GetPropertyValue("Name"); got != "Alice" {
		t.Fatalf("expected default field name for empty JSON tag name, got %v", got)
	}
	if !state.IsNull("Ignored") || !state.IsNull("-") {
		t.Fatal("json ignored field was included in snapshot state")
	}
}

func TestEntityTagUnwrapsInterfaceAndPointerChains(t *testing.T) {
	type holder struct {
		ID      int `govers:"id"`
		Address any `govers:"entity" json:"address"`
	}

	address := &Address{ID: 42}
	addressPointer := &address
	state, err := NewSnapshotFactory().ExtractState(&holder{ID: 1, Address: addressPointer})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := state.GetPropertyValue("address"), NewInstanceID("Address", 42).Value(); got != want {
		t.Fatalf("expected entity reference %q, got %v", want, got)
	}
}

func TestEntityTagUsesOriginalReferenceForTypeName(t *testing.T) {
	type holder struct {
		ID      int      `govers:"id"`
		Address *Address `govers:"entity" json:"address"`
	}

	factory := NewSnapshotFactory().WithTypeNameFunc(func(obj any) string {
		switch obj.(type) {
		case *Address:
			return "Location"
		default:
			return GetTypeName(obj)
		}
	})
	state, err := factory.ExtractState(holder{ID: 1, Address: &Address{ID: 42}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := state.GetPropertyValue("address"), NewInstanceID("Location", 42).Value(); got != want {
		t.Fatalf("expected custom entity type %q, got %v", want, got)
	}
}

func TestInvalidEntityTagReturnsError(t *testing.T) {
	type invalid struct {
		ID        int    `govers:"id"`
		Reference string `govers:"entity"`
	}
	_, err := NewSnapshotFactory().ExtractState(invalid{ID: 1, Reference: "not-an-entity"})
	if !errors.Is(err, ErrInvalidEntityReference) {
		t.Fatalf("expected invalid entity reference error, got %v", err)
	}
}

func TestDuplicateSnapshotPropertyReturnsError(t *testing.T) {
	invalidType := reflect.StructOf([]reflect.StructField{
		{Name: "ID", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(`govers:"id"`)},
		{Name: "First", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"name"`)},
		{Name: "Last", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"name"`)},
	})
	if _, err := NewSnapshotFactory().ExtractState(reflect.New(invalidType).Elem().Interface()); err == nil {
		t.Fatal("expected duplicate snapshot property error")
	}
}

func TestTypeNameUnwrapsPointerChains(t *testing.T) {
	address := &Address{}
	addressPointer := &address
	if got := GetTypeName(addressPointer); got != "Address" {
		t.Fatalf("unexpected type name: %q", got)
	}
	if got := GetTypeNameWithPackage(addressPointer); !strings.HasSuffix(got, ".Address") {
		t.Fatalf("unexpected qualified type name: %q", got)
	}
}

func TestSnapshotStateCopiesMutableValues(t *testing.T) {
	tags := []string{"go", "audit"}
	metadata := map[string]int{"priority": 1}
	state := NewSnapshotState(map[string]any{"tags": tags, "metadata": metadata})

	tags[0] = "changed"
	metadata["priority"] = 2

	if got := state.GetPropertyValue("tags").([]string)[0]; got != "go" {
		t.Fatalf("snapshot slice changed through source alias: %q", got)
	}
	if got := state.GetPropertyValue("metadata").(map[string]int)["priority"]; got != 1 {
		t.Fatalf("snapshot map changed through source alias: %d", got)
	}
}

func TestSnapshotStateCopiesCyclicPointers(t *testing.T) {
	type node struct {
		Value string
		Next  *node
	}
	original := &node{Value: "root"}
	original.Next = original
	state := NewSnapshotState(map[string]any{"node": original})
	cloned := state.GetPropertyValue("node").(*node)
	if cloned == original || cloned.Next != cloned {
		t.Fatal("cyclic pointer graph was not cloned independently")
	}
}

func TestSnapshotStatePreservesExplicitNilValue(t *testing.T) {
	state := NewSnapshotState(map[string]any{"optional": nil})
	if state.Size() != 1 || state.IsNull("optional") || state.GetPropertyValue("optional") != nil {
		t.Fatalf("explicit nil property was not preserved: %s", state.String())
	}
	if !state.Clone().Equals(state) {
		t.Fatal("state with explicit nil value did not clone correctly")
	}
}

func TestSnapshotStatesWithDifferentNilPropertiesAreNotEqual(t *testing.T) {
	left := NewSnapshotState(map[string]any{"left": nil})
	right := NewSnapshotState(map[string]any{"right": nil})
	if left.Equals(right) {
		t.Fatal("states with different property names compared equal")
	}
}

func TestNumericComparisonPreservesLargeIntegerPrecision(t *testing.T) {
	const first = int64(1 << 53)
	oldState := NewSnapshotState(map[string]any{"value": first})
	newState := NewSnapshotState(map[string]any{"value": first + 1})
	if StatesEqual(oldState, newState) {
		t.Fatal("different integers above float64's exact range compared equal")
	}
}

func TestMapComparisonAcrossNumericKeyTypes(t *testing.T) {
	left := NewSnapshotState(map[string]any{"values": map[int]string{1: "one"}})
	right := NewSnapshotState(map[string]any{"values": map[int64]string{1: "one"}})
	if !StatesEqual(left, right) {
		t.Fatal("maps with numerically equivalent keys should compare equal")
	}
}

func TestQueryValidation(t *testing.T) {
	tests := []Query{
		{},
		{Type: QueryByInstanceID},
		{Type: QueryByClass},
		{Type: QueryAny, Limit: -1},
		{Type: QueryAny, Skip: -1},
		{Type: QueryAny, Version: -1},
		{Type: QueryAny, FromDate: time.Now(), ToDate: time.Now().Add(-time.Hour)},
	}
	for _, query := range tests {
		if err := query.Validate(); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("expected invalid query error for %+v, got %v", query, err)
		}
	}
	if err := AnyDomainObjectQuery().Build().Validate(); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
}

type PersonWithIgnore struct {
	ID           int    `govers:"id"`
	Name         string `json:"name"`
	InternalNote string `govers:"ignore"`
	TempData     string `govers:"ignore" json:"tempData"`
}

type PersonWithEntity struct {
	ID      int      `govers:"id"`
	Name    string   `json:"name"`
	Address *Address `govers:"entity" json:"address"`
}

type PersonWithIgnoreOrder struct {
	ID   int      `govers:"id"`
	Name string   `json:"name"`
	Tags []string `govers:"ignoreOrder" json:"tags"`
}

type PersonWithMultipleTags struct {
	ID           int      `govers:"id"`
	Name         string   `json:"name"`
	InternalNote string   `govers:"ignore" json:"internalNote"`
	Address      *Address `govers:"entity" json:"address"`
	Tags         []string `govers:"ignoreOrder" json:"tags"`
}

type PersonWithCombinedTags struct {
	ID      int      `govers:"id,primary"`
	Name    string   `json:"name"`
	Address *Address `govers:"entity,nullable" json:"address"`
	Tags    []string `govers:"ignoreOrder,set" json:"tags"`
	Secret  string   `govers:"ignore,audit" json:"secret"`
}

func TestIgnoreTag(t *testing.T) {
	factory := NewSnapshotFactory()

	person := PersonWithIgnore{
		ID:           1,
		Name:         testPersonName,
		InternalNote: "This should be ignored",
		TempData:     "Also ignored",
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if state.GetPropertyValue("name") != testPersonName {
		t.Errorf("Expected name '%s', got '%v'", testPersonName, state.GetPropertyValue("name"))
	}

	if state.GetPropertyValue("ID") != 1 {
		t.Errorf("Expected ID 1, got '%v'", state.GetPropertyValue("ID"))
	}

	// Verify ignored fields are not captured in state
	if val := state.GetPropertyValue("InternalNote"); val != nil {
		t.Errorf("Expected InternalNote to be nil (ignored), got '%v'", val)
	}

	if val := state.GetPropertyValue("tempData"); val != nil {
		t.Errorf("Expected tempData to be nil (ignored), got '%v'", val)
	}
}

func TestIgnoreTagWithComparison(t *testing.T) {
	factory := NewSnapshotFactory()

	person1 := PersonWithIgnore{
		ID:           1,
		Name:         testPersonName,
		InternalNote: "Note 1",
		TempData:     "Temp 1",
	}

	person2 := PersonWithIgnore{
		ID:           1,
		Name:         testPersonName,
		InternalNote: "Note 2 - different!",
		TempData:     "Temp 2 - also different!",
	}

	state1, err := factory.ExtractState(person1)
	if err != nil {
		t.Fatalf("Failed to extract state 1: %v", err)
	}

	state2, err := factory.ExtractState(person2)
	if err != nil {
		t.Fatalf("Failed to extract state 2: %v", err)
	}

	if !StatesEqual(state1, state2) {
		t.Errorf("Expected states to be equal when only ignored fields differ")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 0 {
		t.Errorf("Expected no changed properties, got %v", changedProps)
	}
}

func TestEntityTag(t *testing.T) {
	factory := NewSnapshotFactory()

	address := &Address{
		ID:     100,
		Street: "123 Main St",
		City:   "Boston",
	}

	person := PersonWithEntity{
		ID:      1,
		Name:    testPersonName,
		Address: address,
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if state.GetPropertyValue("name") != testPersonName {
		t.Errorf("Expected name '%s', got '%v'", testPersonName, state.GetPropertyValue("name"))
	}

	addressValue := state.GetPropertyValue("address")
	if addressValue == nil {
		t.Fatal("Expected address to be present")
	}

	addressStr, ok := addressValue.(string)
	if !ok {
		t.Fatalf("Expected address to be dehydrated to a string, got %T", addressValue)
	}

	expectedGlobalID := NewInstanceID("Address", 100).Value()
	if addressStr != expectedGlobalID {
		t.Errorf("Expected address GlobalId '%s', got '%s'", expectedGlobalID, addressStr)
	}
}

func TestEntityTagWithComparison(t *testing.T) {
	factory := NewSnapshotFactory()

	address1 := &Address{ID: 100, Street: "123 Main St", City: "Boston"}
	address2 := &Address{ID: 200, Street: "456 Oak Ave", City: "Chicago"}

	person1 := PersonWithEntity{ID: 1, Name: "John", Address: address1}
	person2 := PersonWithEntity{ID: 1, Name: "John", Address: address2}

	state1, err := factory.ExtractState(person1)
	if err != nil {
		t.Fatalf("Failed to extract state 1: %v", err)
	}

	state2, err := factory.ExtractState(person2)
	if err != nil {
		t.Fatalf("Failed to extract state 2: %v", err)
	}

	if StatesEqual(state1, state2) {
		t.Error("Expected states to be different when address reference changes")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 1 || changedProps[0] != "address" {
		t.Errorf("Expected changed property 'address', got %v", changedProps)
	}
}

func TestEntityTagWithNilValue(t *testing.T) {
	factory := NewSnapshotFactory()

	person := PersonWithEntity{
		ID:      1,
		Name:    testPersonName,
		Address: nil,
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if state.GetPropertyValue("address") != nil {
		t.Errorf("Expected nil address to be skipped, got '%v'", state.GetPropertyValue("address"))
	}
}

func TestIgnoreOrderTag(t *testing.T) {
	factory := NewSnapshotFactory()

	person := PersonWithIgnoreOrder{
		ID:   1,
		Name: testPersonName,
		Tags: []string{"developer", "go", "backend"},
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if !state.ShouldIgnoreOrder("tags") {
		t.Error("Expected 'tags' property to have ignoreOrder flag set")
	}

	if state.ShouldIgnoreOrder("name") {
		t.Error("Did not expect 'name' to have ignoreOrder flag")
	}
}

func TestIgnoreOrderTagWithComparison(t *testing.T) {
	factory := NewSnapshotFactory()

	person1 := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John",
		Tags: []string{"developer", "go", "backend"},
	}

	person2 := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John",
		Tags: []string{"backend", "developer", "go"},
	}

	state1, err := factory.ExtractState(person1)
	if err != nil {
		t.Fatalf("Failed to extract state 1: %v", err)
	}

	state2, err := factory.ExtractState(person2)
	if err != nil {
		t.Fatalf("Failed to extract state 2: %v", err)
	}

	if !StatesEqual(state1, state2) {
		t.Error("Expected states to be equal when slice order differs but elements are the same")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 0 {
		t.Errorf("Expected no changed properties, got %v", changedProps)
	}
}

func TestIgnoreOrderTagWithDifferentElements(t *testing.T) {
	factory := NewSnapshotFactory()

	person1 := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John",
		Tags: []string{"developer", "go", "backend"},
	}

	person2 := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John",
		Tags: []string{"developer", "python", "backend"},
	}

	state1, err := factory.ExtractState(person1)
	if err != nil {
		t.Fatalf("Failed to extract state 1: %v", err)
	}

	state2, err := factory.ExtractState(person2)
	if err != nil {
		t.Fatalf("Failed to extract state 2: %v", err)
	}

	if StatesEqual(state1, state2) {
		t.Error("Expected states to be different when slice elements differ")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 1 || changedProps[0] != "tags" {
		t.Errorf("Expected changed property 'tags', got %v", changedProps)
	}
}

func TestIgnoreOrderWithoutTag(t *testing.T) {
	type PersonWithOrderedTags struct {
		ID   int      `govers:"id"`
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}

	factory := NewSnapshotFactory()

	person1 := PersonWithOrderedTags{
		ID:   1,
		Name: "John",
		Tags: []string{"a", "b", "c"},
	}

	person2 := PersonWithOrderedTags{
		ID:   1,
		Name: "John",
		Tags: []string{"c", "b", "a"},
	}

	state1, err := factory.ExtractState(person1)
	if err != nil {
		t.Fatalf("Failed to extract state 1: %v", err)
	}

	state2, err := factory.ExtractState(person2)
	if err != nil {
		t.Fatalf("Failed to extract state 2: %v", err)
	}

	if StatesEqual(state1, state2) {
		t.Error("Expected states to be different when slice order differs for non-ignoreOrder slices")
	}
}

func TestMultipleTagsOnEntity(t *testing.T) {
	factory := NewSnapshotFactory()

	address := &Address{ID: 100, Street: "123 Main St", City: "Boston"}

	person := PersonWithMultipleTags{
		ID:           1,
		Name:         testPersonName,
		InternalNote: "Secret note",
		Address:      address,
		Tags:         []string{"developer", "go"},
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if state.GetPropertyValue("name") != testPersonName {
		t.Errorf("Expected name '%s', got '%v'", testPersonName, state.GetPropertyValue("name"))
	}

	if state.GetPropertyValue("internalNote") != nil {
		t.Error("Expected internalNote to be ignored")
	}

	addressValue := state.GetPropertyValue("address")
	if addressValue == nil {
		t.Fatal("Expected address to be present")
	}
	if _, ok := addressValue.(string); !ok {
		t.Errorf("Expected address to be dehydrated to string, got %T", addressValue)
	}

	if !state.ShouldIgnoreOrder("tags") {
		t.Error("Expected tags to have ignoreOrder flag")
	}
}

func TestCombinedGoversTags(t *testing.T) {
	factory := NewSnapshotFactory()

	address := &Address{ID: 100, Street: "123 Main St", City: "Boston"}
	person := PersonWithCombinedTags{
		ID:      1,
		Name:    testPersonName,
		Address: address,
		Tags:    []string{"go", "backend"},
		Secret:  "hidden",
	}

	globalID, err := factory.ExtractGlobalID(person)
	if err != nil {
		t.Fatalf("Failed to extract global ID: %v", err)
	}
	if globalID.Value() != "PersonWithCombinedTags/1" {
		t.Fatalf("Expected combined id tag to be honored, got %s", globalID.Value())
	}

	state, err := factory.ExtractState(person)
	if err != nil {
		t.Fatalf("Failed to extract state: %v", err)
	}

	if state.GetPropertyValue("secret") != nil {
		t.Error("Expected combined ignore tag to omit secret")
	}
	if state.GetPropertyValue("address") != NewInstanceID("Address", 100).Value() {
		t.Errorf("Expected combined entity tag to dehydrate address, got %v", state.GetPropertyValue("address"))
	}
	if !state.ShouldIgnoreOrder("tags") {
		t.Error("Expected combined ignoreOrder tag to be honored")
	}
}

func TestNumericComparison(t *testing.T) {
	state1 := NewSnapshotState(map[string]any{
		"salary": 50000,
		"bonus":  1000.50,
		"age":    int64(30),
	})

	state2 := NewSnapshotState(map[string]any{
		"salary": float64(50000),
		"bonus":  1000.50,
		"age":    float64(30),
	})

	if !StatesEqual(state1, state2) {
		t.Error("Expected states with equivalent numeric values to be equal")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 0 {
		t.Errorf("Expected no changed properties, got %v", changedProps)
	}
}

func TestParseCommitID(t *testing.T) {
	commitID, err := ParseCommitID("12.10")
	if err != nil {
		t.Fatalf("ParseCommitID failed: %v", err)
	}
	if commitID != (CommitID{MajorID: 12, MinorID: 10}) {
		t.Fatalf("Expected 12.10, got %v", commitID)
	}

	if _, err := ParseCommitID("12"); err == nil {
		t.Fatal("Expected invalid commit ID to return an error")
	}
}

func TestSnapshotStateJSONRoundTripIgnoreOrder(t *testing.T) {
	state := NewSnapshotStateBuilder().
		WithIgnoreOrderProperty("tags").
		WithPropertyValue("tags", []string{"a", "b"}).
		WithPropertyValue("name", "John").
		Build()

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var restored SnapshotState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !restored.ShouldIgnoreOrder("tags") {
		t.Fatalf("expected ignoreOrder metadata to be preserved")
	}

	if restored.GetPropertyValue("name") != "John" {
		t.Fatalf("expected name to survive round-trip, got %v", restored.GetPropertyValue("name"))
	}
}

func TestSnapshotStateJSONRoundTripEntityMetadata(t *testing.T) {
	state := NewSnapshotStateBuilder().
		WithEntityProperty("address").
		WithPropertyValue("address", "Address/42").
		Build()

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var decoded SnapshotState
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.IsEntityReference("address") {
		t.Fatal("entity-reference metadata was lost during JSON round trip")
	}
}

func TestNumericComparisonWithActualChange(t *testing.T) {
	state1 := NewSnapshotState(map[string]any{
		"salary": 50000,
	})

	state2 := NewSnapshotState(map[string]any{
		"salary": float64(60000),
	})

	if StatesEqual(state1, state2) {
		t.Error("Expected states with different numeric values to be different")
	}

	changedProps := CompareStates(state1, state2)
	if len(changedProps) != 1 || changedProps[0] != "salary" {
		t.Errorf("Expected changed property 'salary', got %v", changedProps)
	}
}
