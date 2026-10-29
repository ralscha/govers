package core

import (
	"encoding/json/v2"
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSnapshotStatePreservesSubsliceLengths(t *testing.T) {
	backing := []string{"first", "second", "third"}
	state := NewSnapshotState(map[string]any{
		"empty": backing[:0],
		"short": backing[:1],
		"full":  backing,
	})
	backing[0] = "modified"
	for name, want := range map[string][]string{
		"empty": {},
		"short": {"first"},
		"full":  {"first", "second", "third"},
	} {
		if got := state.GetPropertyValue(name); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %#v, want %#v", name, got, want)
		}
	}
}

func TestSnapshotStateComparesCyclicCollections(t *testing.T) {
	makeMap := func(value int) map[string]any {
		result := map[string]any{"value": value}
		result["self"] = result
		return result
	}
	makeSlice := func(value int) []any {
		result := make([]any, 2)
		result[0] = result
		result[1] = value
		return result
	}
	tests := []struct {
		name      string
		makeValue func(int) any
	}{
		{"map", func(value int) any { return makeMap(value) }},
		{"slice", func(value int) any { return makeSlice(value) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := NewSnapshotState(map[string]any{"value": tt.makeValue(1)})
			same := NewSnapshotState(map[string]any{"value": tt.makeValue(1)})
			different := NewSnapshotState(map[string]any{"value": tt.makeValue(2)})
			if !first.Equals(same) || !same.Equals(first) {
				t.Fatal("equivalent cyclic collections compared unequal")
			}
			if first.Equals(different) || different.Equals(first) {
				t.Fatal("different cyclic collections compared equal")
			}
			if got := different.DifferentValues(first); !reflect.DeepEqual(got, []string{"value"}) {
				t.Fatalf("unexpected changed properties: %v", got)
			}
		})
	}
}

func TestMapComparisonWithEquivalentNumericInterfaceKeys(t *testing.T) {
	left := map[any]string{int(1): "first", int64(1): "second"}
	right := map[any]string{int32(1): "second", uint(1): "first"}
	for range 100 {
		if !valuesEqual(left, right) || !valuesEqual(right, left) {
			t.Fatal("equivalent numeric keys must match their corresponding values")
		}
	}
	if valuesEqual(left, map[any]string{int32(1): "first", uint(1): "first"}) {
		t.Fatal("different values compared equal")
	}
}

func TestNumericComparisonBoundaries(t *testing.T) {
	tests := []struct {
		left, right any
		equal       bool
	}{
		{int64(math.MaxInt64), uint64(math.MaxInt64), true},
		{int64(-1), uint64(math.MaxUint64), false},
		{uint64(math.MaxUint64), int64(-1), false},
		{uint64(math.MaxUint64), uint64(math.MaxUint64 - 1), false},
		{int64(1 << 53), float64(1 << 53), true},
		{int64(1<<53 + 1), float64(1 << 53), false},
		{float32(0.5), float64(0.5), true},
		{math.Inf(1), math.Inf(1), true},
		{math.NaN(), math.NaN(), false},
	}
	for _, tt := range tests {
		if got := valuesEqual(tt.left, tt.right); got != tt.equal {
			t.Errorf("valuesEqual(%v (%T), %v (%T)) = %v, want %v", tt.left, tt.left, tt.right, tt.right, got, tt.equal)
		}
	}
}

func TestExtractGlobalIDWithNilEmbeddedPointer(t *testing.T) {
	type identity struct{ ID string }
	type object struct{ *identity }
	factory := NewSnapshotFactory()
	if _, err := factory.ExtractGlobalID(object{}); !errors.Is(err, ErrNoIDField) {
		t.Fatalf("expected ErrNoIDField, got %v", err)
	}
	id, err := factory.ExtractGlobalID(object{identity: &identity{ID: "123"}})
	if err != nil || id.Value() != "object/123" {
		t.Fatalf("unexpected promoted ID: %v, error: %v", id, err)
	}
}

func TestSnapshotStateJSONPreservesLargeIntegers(t *testing.T) {
	state := NewSnapshotState(map[string]any{
		"signed":   int64(1<<53 + 1),
		"unsigned": uint64(math.MaxUint64),
		"nested":   []any{map[string]any{"minimum": int64(math.MinInt64)}},
	})
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored SnapshotState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !state.Equals(restored) || !restored.Equals(state) {
		t.Fatalf("integer values changed across JSON round trip: %s", restored.String())
	}
	changed := NewSnapshotState(map[string]any{
		"signed":   int64(1<<53 + 2),
		"unsigned": uint64(math.MaxUint64),
		"nested":   []any{map[string]any{"minimum": int64(math.MinInt64)}},
	})
	if got := restored.DifferentValues(changed); !reflect.DeepEqual(got, []string{"signed"}) {
		t.Fatalf("expected signed integer change, got %v", got)
	}
}

func TestSnapshotStateJSONComparesTypedValues(t *testing.T) {
	type status string
	type enabled bool
	type detail struct {
		Label string `json:"label"`
		Count int64  `json:"count"`
	}
	when := time.Date(2026, 10, 29, 13, 14, 15, 123456789, time.FixedZone("test", 3600))
	state := NewSnapshotState(map[string]any{
		"detail":      detail{Label: "example", Count: 1<<53 + 1},
		"details":     []detail{{Label: "nested", Count: 1<<53 + 2}},
		"when":        when,
		"bytes":       []byte{1, 2, 3},
		"mapped":      map[string]detail{"key": {Label: "map"}},
		"integerKeys": map[int64]string{1<<53 + 1: "large"},
		"namedKeys":   map[status]string{"open": "value"},
		"status":      status("open"),
		"enabled":     enabled(true),
	})
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored SnapshotState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !state.Equals(restored) || !restored.Equals(state) {
		t.Fatalf("typed values compare differently after JSON: %v", state.DifferentValues(restored))
	}
	restored.properties["detail"].(map[string]any)["label"] = "changed"
	if got := state.DifferentValues(restored); !reflect.DeepEqual(got, []string{"detail"}) {
		t.Fatalf("expected nested struct change, got %v", got)
	}
}

func TestSnapshotStateTimeEqualityIgnoresLocationAndMonotonicClock(t *testing.T) {
	when := time.Now()
	left := NewSnapshotState(map[string]any{"when": when})
	right := NewSnapshotState(map[string]any{"when": when.UTC()})
	if !left.Equals(right) || !right.Equals(left) {
		t.Fatal("the same instant in different locations compared unequal")
	}
	persisted := NewSnapshotState(map[string]any{"when": when.UTC().Format(time.RFC3339Nano)})
	if !left.Equals(persisted) || !persisted.Equals(left) {
		t.Fatal("persisted time in a different location compared unequal")
	}
}

func TestSnapshotStateJSONPreservesNestedNullPointers(t *testing.T) {
	state := NewSnapshotState(map[string]any{
		"map":       map[string]*int{"missing": nil},
		"slice":     []*int{nil},
		"interface": []any{(*int)(nil)},
		"nilMap":    map[string]int(nil),
		"nilSlice":  []int(nil),
	})
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored SnapshotState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !state.Equals(restored) || !restored.Equals(state) {
		t.Fatalf("nested null pointers changed across JSON: %v", state.DifferentValues(restored))
	}
	restored.properties["map"].(map[string]any)["missing"] = int64(1)
	if got := state.DifferentValues(restored); !reflect.DeepEqual(got, []string{"map"}) {
		t.Fatalf("expected null pointer replacement, got %v", got)
	}
	if valuesEqual(map[string]int(nil), nil) || valuesEqual([]int(nil), nil) {
		t.Fatal("nil collections must remain distinct from JSON null")
	}
}

func BenchmarkSnapshotStateMapEquality(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			values := make(map[string]int, size)
			for i := range size {
				values[strconv.Itoa(i)] = i
			}
			left := NewSnapshotState(map[string]any{"values": values})
			right := left.Clone()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if !left.Equals(right) {
					b.Fatal("states differ")
				}
			}
		})
	}
}
