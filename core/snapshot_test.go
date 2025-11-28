package core

import (
	"encoding/json"
	"testing"
)

const testPersonName = "John Doe"

type Address struct {
	ID     int    `govers:"id"`
	Street string `json:"street"`
	City   string `json:"city"`
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
