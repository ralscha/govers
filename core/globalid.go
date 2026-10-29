package core

import (
	"fmt"
	"reflect"
)

// GlobalID uniquely identifies a domain object across commits.
// It can represent either an Entity (via InstanceID) or a ValueObject (via ValueObjectID).
type GlobalID interface {
	// Value returns the string representation of this GlobalID.
	// For InstanceID: "TypeName/localId" (e.g., "Employee/123")
	// For ValueObjectID: "TypeName/localId#fragment" (e.g., "Employee/123#address")
	Value() string

	// TypeName returns the type name of the object this GlobalID refers to.
	TypeName() string

	// isGlobalID is a marker method to prevent external implementations.
	isGlobalID()
}

// InstanceID identifies an Entity instance by its type and local ID.
// Entities are objects with identity - they are compared by their ID, not their properties.
type InstanceID struct {
	typeName string
	cdoID    any // The local ID value (e.g., int, string, uuid)
	value    string
}

// NewInstanceID creates a new InstanceID for an entity. Mutable local ID values
// are copied, and the formatted identity is fixed at construction time.
func NewInstanceID(typeName string, cdoID any) InstanceID {
	return InstanceID{
		typeName: typeName,
		cdoID:    cloneLocalID(cdoID),
		value:    fmt.Sprintf("%s/%v", typeName, cdoID),
	}
}

// Value returns the string representation: "TypeName/localId"
func (id InstanceID) Value() string {
	if id.value != "" {
		return id.value
	}
	return fmt.Sprintf("%s/%v", id.typeName, id.cdoID)
}

// TypeName returns the type name of the entity.
func (id InstanceID) TypeName() string {
	return id.typeName
}

// CdoID returns an independent copy of the local ID value.
func (id InstanceID) CdoID() any {
	return cloneLocalID(id.cdoID)
}

func (id InstanceID) isGlobalID() {}

func cloneLocalID(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflectValue(reflect.ValueOf(value), make(map[cloneVisit]reflect.Value)).Interface()
}

// ValueObjectID identifies a ValueObject by its owner entity and a fragment path.
// ValueObjects are objects without identity - they are compared by their properties.
type ValueObjectID struct {
	ownerID  InstanceID
	typeName string
	fragment string // Property path from owner, e.g., "address" or "address.city"
}

// NewValueObjectID creates a new ValueObjectID for a value object.
func NewValueObjectID(typeName string, ownerID InstanceID, fragment string) ValueObjectID {
	return ValueObjectID{
		typeName: typeName,
		ownerID:  ownerID,
		fragment: fragment,
	}
}

// Value returns the string representation: "OwnerTypeName/ownerId#fragment"
func (id ValueObjectID) Value() string {
	return fmt.Sprintf("%s#%s", id.ownerID.Value(), id.fragment)
}

// TypeName returns the type name of the value object.
func (id ValueObjectID) TypeName() string {
	return id.typeName
}

// OwnerID returns the InstanceID of the owning entity.
func (id ValueObjectID) OwnerID() InstanceID {
	return id.ownerID
}

// Fragment returns the property path from the owner to this value object.
func (id ValueObjectID) Fragment() string {
	return id.fragment
}

func (id ValueObjectID) isGlobalID() {}

// Ensure types implement GlobalID
var (
	_ GlobalID = InstanceID{}
	_ GlobalID = ValueObjectID{}
)
