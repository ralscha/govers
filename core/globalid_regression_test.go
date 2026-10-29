package core_test

import (
	"fmt"
	"testing"

	"github.com/ralscha/govers/core"
)

type mutableLocalID struct {
	Name string
}

func (id *mutableLocalID) String() string { return id.Name }

func TestInstanceIDOwnsMutableLocalID(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		mutate func(any)
	}{
		{"slice", []byte{1, 2}, func(value any) { value.([]byte)[0] = 9 }},
		{"map", map[string]int{"id": 1}, func(value any) { value.(map[string]int)["id"] = 9 }},
		{"stringer", &mutableLocalID{Name: "original"}, func(value any) { value.(*mutableLocalID).Name = "changed" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantLocal := fmt.Sprint(tt.value)
			id := core.NewInstanceID("Object", tt.value)
			ownerID := core.NewValueObjectID("Address", id, "address")
			tt.mutate(tt.value)
			tt.mutate(id.CdoID())
			tt.mutate(ownerID.OwnerID().CdoID())
			if got := fmt.Sprint(id.CdoID()); got != wantLocal {
				t.Fatalf("local ID was mutated: got %s, want %s", got, wantLocal)
			}
			if got := id.Value(); got != "Object/"+wantLocal {
				t.Fatalf("identity changed: %s", got)
			}
			if got := ownerID.Value(); got != "Object/"+wantLocal+"#address" {
				t.Fatalf("value object identity changed: %s", got)
			}
		})
	}
}

func TestInstanceIDPreservesPointerIdentityText(t *testing.T) {
	value := 42
	want := fmt.Sprintf("Object/%v", &value)
	id := core.NewInstanceID("Object", &value)
	value = 99
	localIDCopy := id.CdoID().(*int)
	if *localIDCopy != 42 || localIDCopy == &value {
		t.Fatalf("local pointer was not copied: %v", localIDCopy)
	}
	if got := id.Value(); got != want {
		t.Fatalf("pointer identity format changed: got %s, want %s", got, want)
	}
	*localIDCopy = 100
	if *id.CdoID().(*int) != 42 {
		t.Fatal("returned pointer changed the stored local ID")
	}
}

func TestZeroInstanceIDValue(t *testing.T) {
	var id core.InstanceID
	if got := id.Value(); got != "/<nil>" {
		t.Fatalf("zero-value identity format changed: %s", got)
	}
}
