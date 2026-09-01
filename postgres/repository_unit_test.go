package postgres

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ralscha/govers/core"
)

func TestGlobalIDConditionIncludesValueObjectOwnerType(t *testing.T) {
	owner := core.NewInstanceID("Customer", 7)
	valueObject := core.NewValueObjectID("Address", owner, "billing")
	condition, args, err := globalIDCondition(valueObject, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(condition, "owner.type_name = $6") || !strings.Contains(condition, "owner.fragment IS NULL") {
		t.Fatalf("owner identity is incomplete: %s", condition)
	}
	want := []any{"Address", "billing", "7", "Customer"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("unexpected condition args: got %v, want %v", args, want)
	}
}

func TestBuildSnapshotIncludesOwnerAndCommitProperties(t *testing.T) {
	fragment := "billing"
	ownerID := "7"
	ownerType := "Customer"
	snapshot, err := buildSnapshot(
		ownerID,
		"Address",
		&fragment,
		&ownerID,
		&ownerType,
		[]byte(`{"properties":{"city":"Zurich"}}`),
		[]byte(`["city"]`),
		string(core.Update),
		2,
		"3.00",
		"author",
		time.Now(),
		[]byte(`{"source":"test"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	valueObject, ok := snapshot.GlobalID.(core.ValueObjectID)
	if !ok {
		t.Fatalf("expected value object ID, got %T", snapshot.GlobalID)
	}
	if valueObject.OwnerID().TypeName() != ownerType || valueObject.OwnerID().CdoID() != ownerID {
		t.Fatalf("unexpected owner: %#v", valueObject.OwnerID())
	}
	if got := snapshot.CommitMetadata.Properties["source"]; got != "test" {
		t.Fatalf("commit property missing: %q", got)
	}
}
