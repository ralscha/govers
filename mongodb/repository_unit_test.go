package mongodb

import (
	"errors"
	"testing"
	"time"

	"github.com/ralscha/govers/core"
	"go.mongodb.org/mongo-driver/v2/bson"
)

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
	document := repository.snapshotToDocument(snapshot)
	decoded, err := repository.documentToSnapshot(document)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.State.IsEntityReference("address") {
		t.Fatal("entity-reference metadata was lost in MongoDB conversion")
	}
}
