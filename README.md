# govers

A Go library for object auditing and versioning, inspired by [JaVers](https://javers.org/).

## Features

- Track changes to domain objects over time
- Create `INITIAL`, `UPDATE`, and `TERMINAL` snapshots for object state
- Query snapshots by instance, type, author, commit ID, date range, version, changed property, limit, and offset
- Use `govers` struct tags for IDs, ignored fields, entity references, and order-insensitive slices
- Get typed value, reference, list, and map changes for each successful commit
- Store snapshots with in-memory, PostgreSQL, or MongoDB repositories

## Installation

```bash
go get github.com/ralscha/govers/core
```

Repository backends are separate modules:

```bash
go get github.com/ralscha/govers/inmemory
go get github.com/ralscha/govers/postgres
go get github.com/ralscha/govers/mongodb
```

## Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/ralscha/govers/core"
	"github.com/ralscha/govers/inmemory"
)

type User struct {
	ID   string `govers:"id"`
	Name string
}

func main() {
	ctx := context.Background()
	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	user := User{ID: "1", Name: "Alice"}
	_, _ = g.Commit(ctx, "admin", &user)

	user.Name = "Alice Smith"
	_, _ = g.Commit(ctx, "admin", &user)

	query := core.ByInstanceIDQuery("User", "1").Build()
	snapshots, _ := g.FindSnapshots(ctx, query)
	for _, s := range snapshots {
		fmt.Printf("v%d [%s]: %s (changed: %v)\n", s.Version, s.Type, s.State.String(), s.ChangedProperties)
	}

	latest, _ := g.GetLatestSnapshot(ctx, "User", "1")
	fmt.Printf("Latest: %s\n", latest.State.String())
}
```

Example output:

```text
v2 [UPDATE]: {ID:1, Name:Alice Smith} (changed: [Name])
v1 [INITIAL]: {ID:1, Name:Alice} (changed: [])
Latest: {ID:1, Name:Alice Smith}
```

Committing an unchanged object returns `nil, nil`. Deleting an object creates a terminal snapshot; a later commit for the same ID returns `core.ErrObjectDeleted` rather than silently resurrecting it. To attach metadata to a deletion, use `DeleteWithProperties`:

```go
_, err := g.DeleteWithProperties(ctx, "admin", &user, map[string]string{
	"reason": "account closed",
})
```

Snapshot state owns independent copies of mutable maps, slices, and pointers, so mutating a domain object after a commit does not rewrite in-memory history.

Repositories protect their global commit sequence when multiple `Govers` instances write concurrently. A losing writer receives an error wrapping `core.ErrConcurrentCommit`; retry the complete commit operation against the new head.

## Tags

- `govers:"id"` marks the object ID field. Fields named `ID`, `Id`, `id`, `Uuid`, `UUID`, or `uuid` are also accepted.
- `govers:"ignore"` excludes a field from snapshots and comparisons.
- `govers:"entity"` stores a referenced entity as its global ID instead of embedding the whole object.
- `govers:"ignoreOrder"` compares slice or array values without considering element order.

Tag options can be combined with commas, for example `govers:"id,primary"`.

## Backends

The in-memory backend needs no setup and is intended for tests and development.

PostgreSQL:

```go
repo, err := postgres.NewWithConnString(ctx, connString)
if err != nil {
	// handle error
}
defer repo.Close()
_ = repo.CreateSchema(ctx)
```

MongoDB:

```go
repo, err := mongodb.NewWithConnString(ctx, connString, "govers")
if err != nil {
	// handle error
}
defer repo.Close(ctx)
_ = repo.EnsureSchema(ctx)
```

## Development

This repository is a Go workspace with separate modules for `core`, `inmemory`, `mongodb`, `postgres`, and `demo`. From the repository root, use the Taskfile module list instead of `go test ./...`:

```bash
task test
task vet
task build
```

Without Task installed:

```bash
go test github.com/ralscha/govers/core/... github.com/ralscha/govers/demo/... github.com/ralscha/govers/inmemory/... github.com/ralscha/govers/mongodb/... github.com/ralscha/govers/postgres/...
```

MongoDB and PostgreSQL tests use Testcontainers and skip when a container provider is unavailable.

Queries are validated consistently by all backends. Negative pagination or version filters, incomplete instance/class queries, and reversed date ranges return an error wrapping `core.ErrInvalidQuery`.

## License

MIT
