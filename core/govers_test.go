package core

import (
	"encoding/json/v2"
	"math"
	"reflect"
	"strconv"
	"testing"
)

type mapTextKey int

func (key mapTextKey) MarshalText() ([]byte, error) {
	return []byte("key-" + strconv.Itoa(int(key))), nil
}

func TestMapChangesAfterJSONRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name   string
		before any
		after  any
		key    any
	}{
		{"integer keys", map[int]string{1: "before", 2: "unchanged"}, map[int]string{1: "after", 2: "unchanged"}, 1},
		{"text keys", map[mapTextKey]string{0: "before", 1: "unchanged"}, map[mapTextKey]string{0: "after", 1: "unchanged"}, mapTextKey(0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := NewSnapshotState(map[string]any{"values": test.before})
			data, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			var restored SnapshotState
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			change := New().createMapChangeFromValues(NewInstanceID("User", 1), CommitMetadata{}, "values", restored.GetPropertyValue("values"), test.after)
			want := []EntryChange{{Key: test.key, Type: ChangedChangeType, Left: "before", Right: "after"}}
			if !reflect.DeepEqual(change.EntryChanges, want) {
				t.Fatalf("map changes = %#v, want %#v", change.EntryChanges, want)
			}
		})
	}
}

func TestMapChangesMatchEquivalentNumericKeys(t *testing.T) {
	g := New()
	change := g.createMapChangeFromValues(NewInstanceID("User", 1), CommitMetadata{}, "counts",
		map[int32]string{1: "before", 2: "removed"},
		map[int64]string{1: "after", 3: "added"})
	want := []EntryChange{
		{Key: int64(1), Type: ChangedChangeType, Left: "before", Right: "after"},
		{Key: int64(3), Type: AddedChangeType, Right: "added"},
		{Key: int32(2), Type: RemovedChangeType, Left: "removed"},
	}
	if !reflect.DeepEqual(change.EntryChanges, want) {
		t.Fatalf("map changes = %#v, want %#v", change.EntryChanges, want)
	}
}

func TestMapChangesPreserveNaNKeyValues(t *testing.T) {
	g := New()
	for _, adding := range []bool{false, true} {
		var before, after map[float64]string
		if adding {
			after = map[float64]string{math.NaN(): "added"}
		} else {
			before = map[float64]string{math.NaN(): "removed"}
		}
		change := g.createMapChangeFromValues(NewInstanceID("User", 1), CommitMetadata{}, "counts", before, after)
		if len(change.EntryChanges) != 1 {
			t.Fatalf("changes = %#v, want one entry", change.EntryChanges)
		}
		entry := change.EntryChanges[0]
		if !math.IsNaN(entry.Key.(float64)) {
			t.Fatalf("key = %v, want NaN", entry.Key)
		}
		if adding && (entry.Type != AddedChangeType || entry.Right != "added") {
			t.Fatalf("addition lost its value: %#v", entry)
		}
		if !adding && (entry.Type != RemovedChangeType || entry.Left != "removed") {
			t.Fatalf("removal lost its value: %#v", entry)
		}
	}
}

func TestMapChangesAreDeterministic(t *testing.T) {
	g := New()
	before := map[string]int{"a": 1, "b": 2, "c": 3}
	after := map[string]int{"b": 4, "c": 3, "d": 5}
	want := []EntryChange{
		{Key: "b", Type: ChangedChangeType, Left: 2, Right: 4},
		{Key: "d", Type: AddedChangeType, Right: 5},
		{Key: "a", Type: RemovedChangeType, Left: 1},
	}
	for range 10 {
		change := g.createMapChangeFromValues(NewInstanceID("User", 1), CommitMetadata{}, "counts", before, after)
		if !reflect.DeepEqual(change.EntryChanges, want) {
			t.Fatalf("map changes = %#v, want %#v", change.EntryChanges, want)
		}
	}
}
