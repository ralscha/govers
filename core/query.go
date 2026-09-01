package core

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidQuery is returned when a snapshot query is incomplete or contains
// contradictory pagination, version, or date filters.
var ErrInvalidQuery = errors.New("invalid query")

// QueryType indicates what kind of query is being performed.
type QueryType string

const (
	// QueryByInstanceID queries for a specific entity instance.
	QueryByInstanceID QueryType = "ByInstanceId"
	// QueryByClass queries for all objects of a given type.
	QueryByClass QueryType = "ByClass"
	// QueryAny queries for any domain object.
	QueryAny QueryType = "Any"
)

// Query represents a query for snapshots or changes.
type Query struct {
	// Type is the kind of query being performed.
	Type QueryType

	// InstanceID is set when querying by instance ID.
	InstanceID *InstanceID

	// TypeName is set when querying by class/type.
	TypeName string

	// Limit is the maximum number of results to return (0 = no limit).
	Limit int

	// Skip is the number of results to skip for pagination.
	Skip int

	// Author filters by commit author (empty = no filter).
	Author string

	// FromDate filters for commits on or after this date (zero = no filter).
	FromDate time.Time

	// ToDate filters for commits on or before this date (zero = no filter).
	ToDate time.Time

	// Version filters for a specific object version (0 = no filter).
	Version int64

	// CommitID filters for a specific commit (zero = no filter).
	CommitID CommitID

	// ChangedProperty filters for snapshots with a specific changed property.
	ChangedProperty string
}

// Validate checks that the query is safe and meaningful for every repository
// implementation. Repository backends call this method as well as Govers so
// direct repository use has the same behavior.
func (q Query) Validate() error {
	if q.Limit < 0 {
		return fmt.Errorf("%w: limit cannot be negative", ErrInvalidQuery)
	}
	if q.Skip < 0 {
		return fmt.Errorf("%w: skip cannot be negative", ErrInvalidQuery)
	}
	if q.Version < 0 {
		return fmt.Errorf("%w: version cannot be negative", ErrInvalidQuery)
	}
	if !q.FromDate.IsZero() && !q.ToDate.IsZero() && q.FromDate.After(q.ToDate) {
		return fmt.Errorf("%w: from date cannot be after to date", ErrInvalidQuery)
	}

	switch q.Type {
	case QueryByInstanceID:
		if q.InstanceID == nil {
			return fmt.Errorf("%w: instance ID is required", ErrInvalidQuery)
		}
		if q.InstanceID.TypeName() == "" {
			return fmt.Errorf("%w: instance type name is required", ErrInvalidQuery)
		}
	case QueryByClass:
		if q.TypeName == "" {
			return fmt.Errorf("%w: class type name is required", ErrInvalidQuery)
		}
	case QueryAny:
		// No identity filter is required.
	default:
		return fmt.Errorf("%w: unknown query type %q", ErrInvalidQuery, q.Type)
	}

	return nil
}

// QueryBuilder provides a fluent API for building queries.
type QueryBuilder struct {
	query Query
}

// ByInstanceIDQuery creates a query for a specific entity instance.
func ByInstanceIDQuery(typeName string, id any) *QueryBuilder {
	instanceID := NewInstanceID(typeName, id)
	return &QueryBuilder{
		query: Query{
			Type:       QueryByInstanceID,
			InstanceID: &instanceID,
			TypeName:   typeName,
		},
	}
}

// ByClassQuery creates a query for all objects of a given type.
func ByClassQuery(typeName string) *QueryBuilder {
	return &QueryBuilder{
		query: Query{
			Type:     QueryByClass,
			TypeName: typeName,
		},
	}
}

// AnyDomainObjectQuery creates a query for any domain object.
func AnyDomainObjectQuery() *QueryBuilder {
	return &QueryBuilder{
		query: Query{
			Type: QueryAny,
		},
	}
}

// Limit sets the maximum number of results to return.
func (qb *QueryBuilder) Limit(n int) *QueryBuilder {
	qb.query.Limit = n
	return qb
}

// Skip sets the number of results to skip (for pagination).
func (qb *QueryBuilder) Skip(n int) *QueryBuilder {
	qb.query.Skip = n
	return qb
}

// ByAuthor filters by commit author.
func (qb *QueryBuilder) ByAuthor(author string) *QueryBuilder {
	qb.query.Author = author
	return qb
}

// From filters for commits on or after the given date.
func (qb *QueryBuilder) From(date time.Time) *QueryBuilder {
	qb.query.FromDate = date
	return qb
}

// To filters for commits on or before the given date.
func (qb *QueryBuilder) To(date time.Time) *QueryBuilder {
	qb.query.ToDate = date
	return qb
}

// WithVersion filters for a specific object version.
func (qb *QueryBuilder) WithVersion(version int64) *QueryBuilder {
	qb.query.Version = version
	return qb
}

// WithCommitID filters for a specific commit.
func (qb *QueryBuilder) WithCommitID(commitID CommitID) *QueryBuilder {
	qb.query.CommitID = commitID
	return qb
}

// WithChangedProperty filters for snapshots where the given property changed.
func (qb *QueryBuilder) WithChangedProperty(propertyName string) *QueryBuilder {
	qb.query.ChangedProperty = propertyName
	return qb
}

// Build returns the constructed Query.
func (qb *QueryBuilder) Build() Query {
	return qb.query
}
