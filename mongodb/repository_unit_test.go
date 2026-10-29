package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ralscha/govers/core"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSnapshotDocumentBSONRoundTrip(t *testing.T) {
	repository := &Repository{}
	state := core.NewSnapshotState(map[string]any{
		"nested": map[string]any{"values": []any{map[string]any{"enabled": true, "count": 7}}},
		"bytes":  []byte{0, 1, 255},
		"date":   time.Date(2026, time.January, 1, 12, 0, 0, 123456789, time.UTC),
		"struct": struct {
			Count int64 `json:"count"`
		}{Count: 9007199254740993},
	})
	pointerID := 42
	for _, id := range []any{0, "", []byte{1, 2}, &pointerID} {
		snapshot := core.NewSnapshot(core.NewInstanceID("Record", id), state, core.Initial, 1,
			core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author"))
		document, err := repository.snapshotToDocument(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		document = SnapshotDocument{}
		if err := bson.Unmarshal(encoded, &document); err != nil {
			t.Fatal(err)
		}
		decoded, err := repository.documentToSnapshot(document)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.GlobalID.Value() != snapshot.GlobalID.Value() {
			t.Errorf("ID changed after BSON round trip: got %q, want %q", decoded.GlobalID.Value(), snapshot.GlobalID.Value())
		}
		if !state.Equals(decoded.State) {
			t.Errorf("unchanged BSON state reports changed properties: %v", state.DifferentValues(decoded.State))
		}
	}
}

func TestPersistRejectsInvalidStateBeforeWriting(t *testing.T) {
	metadata := core.NewCommitMetadata(core.CommitID{MajorID: 1}, "author")
	commit := core.NewCommit(metadata).
		WithSnapshot(core.NewSnapshot(core.NewInstanceID("Record", 1), core.EmptySnapshotState(), core.Initial, 1, metadata)).
		WithSnapshot(core.NewSnapshot(core.NewInstanceID("Record", 2), core.NewSnapshotState(map[string]any{
			"unsupported": make(chan int),
		}), core.Initial, 1, metadata))
	// No database is configured: serialization must fail before any write.
	err := (&Repository{}).Persist(context.Background(), commit)
	if err == nil || !strings.Contains(err.Error(), "failed to marshal snapshot") {
		t.Fatalf("expected serialization error before writing, got %v", err)
	}
}

func TestSnapshotDocumentRejectsNilGlobalID(t *testing.T) {
	var instanceID *core.InstanceID
	var valueObjectID *core.ValueObjectID
	for _, id := range []core.GlobalID{nil, instanceID, valueObjectID} {
		if _, err := (&Repository{}).snapshotToDocument(core.Snapshot{GlobalID: id}); err == nil {
			t.Fatalf("expected invalid ID error for %T", id)
		}
	}
}

func TestLegacyBSONStateRestoresMapsAndScalars(t *testing.T) {
	date := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	document := SnapshotDocument{
		GlobalID:       GlobalIDDocument{Entity: "Record", CdoID: 1},
		CommitMetadata: CommitMetadataDocument{ID: "1.0"},
		State: StateDocument{Properties: bson.M{
			"nested": bson.D{{Key: "values", Value: bson.A{bson.D{{Key: "enabled", Value: true}}}}},
			"bytes":  bson.Binary{Data: []byte{0, 1, 255}},
			"date":   bson.NewDateTimeFromTime(date),
		}},
	}
	snapshot, err := (&Repository{}).documentToSnapshot(document)
	if err != nil {
		t.Fatal(err)
	}
	want := core.NewSnapshotState(map[string]any{
		"nested": map[string]any{"values": []any{map[string]any{"enabled": true}}},
		"bytes":  []byte{0, 1, 255},
		"date":   date,
	})
	if !want.Equals(snapshot.State) {
		t.Fatalf("legacy BSON state changed properties: %v", want.DifferentValues(snapshot.State))
	}
}

func TestSnapshotFilterBySnapshotType(t *testing.T) {
	for _, snapshotType := range []core.SnapshotType{core.Initial, core.Update, core.Terminal} {
		filter, err := snapshotFilter(core.AnyDomainObjectQuery().WithSnapshotType(snapshotType).Build())
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range filter {
			if entry.Key == fieldSnapshotType && entry.Value == string(snapshotType) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing snapshot type %s in filter %v", snapshotType, filter)
		}
	}
}

func TestSnapshotFilterCombinesDateBounds(t *testing.T) {
	from := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	filter, err := snapshotFilter(core.AnyDomainObjectQuery().From(from).To(to).Build())
	if err != nil {
		t.Fatal(err)
	}

	var dateFilters []bson.E
	for _, element := range filter {
		if element.Key == fieldCommitDate {
			dateFilters = append(dateFilters, element)
		}
	}
	if len(dateFilters) != 1 {
		t.Fatalf("expected one combined date filter, got %v", filter)
	}
	rangeDocument, ok := dateFilters[0].Value.(bson.D)
	if !ok || len(rangeDocument) != 2 {
		t.Fatalf("expected both date bounds in one document, got %#v", dateFilters[0].Value)
	}
}

func TestSnapshotFilterRejectsInvalidQuery(t *testing.T) {
	if _, err := snapshotFilter(core.Query{}); !errors.Is(err, core.ErrInvalidQuery) {
		t.Fatalf("expected invalid query error, got %v", err)
	}
}

func TestDocumentToSnapshotRejectsMissingGlobalID(t *testing.T) {
	repository := &Repository{}
	_, err := repository.documentToSnapshot(SnapshotDocument{})
	if err == nil {
		t.Fatal("expected invalid global ID error")
	}
}

func TestSnapshotDocumentPreservesEntityMetadata(t *testing.T) {
	repository := &Repository{}
	state := core.NewSnapshotStateBuilder().
		WithEntityProperty("address").
		WithPropertyValue("address", "Address/42").
		Build()
	snapshot := core.NewSnapshot(
		core.NewInstanceID("Person", 1),
		state,
		core.Update,
		2,
		core.NewCommitMetadata(core.CommitID{MajorID: 2}, "author"),
	)
	document, err := repository.snapshotToDocument(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := repository.documentToSnapshot(document)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.State.IsEntityReference("address") {
		t.Fatal("entity-reference metadata was lost in MongoDB conversion")
	}
}
