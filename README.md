# govers

A Go library for object auditing and versioning, inspired by [JaVers](https://javers.org/).

## Features

- Track changes to domain objects over time
- Create snapshots (INITIAL, UPDATE, TERMINAL) for each object state
- Query historical data and changes
- Pluggable repository backends (in-memory, PostgreSQL, MongoDB included)

## Installation

```bash
go get github.com/ralscha/govers/core
```

For specific repository backends:

```bash
# In-memory
go get github.com/ralscha/govers/inmemory

# PostgreSQL
go get github.com/ralscha/govers/postgres

# MongoDB
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

    // Commit initial state
    user := User{ID: "1", Name: "Alice"}
    g.Commit(ctx, "admin", &user)

    // Commit an update
    user.Name = "Alice Smith"
    g.Commit(ctx, "admin", &user)

    // Query snapshots for this user
    query := core.ByInstanceIDQuery("User", "1").Build()
    snapshots, _ := g.FindSnapshots(ctx, query)
    for _, s := range snapshots {
        fmt.Printf("v%d [%s]: %s (changed: %v)\n", s.Version, s.Type, s.State.String(), s.ChangedProperties)
    }

    // Get latest snapshot directly
    latest, _ := g.GetLatestSnapshot(ctx, "User", "1")
    fmt.Printf("Latest: %s\n", latest.State.String())
}

// Output:
// v2 [UPDATE]: User{ID:1, Name:Alice Smith} (changed: [Name])
// v1 [INITIAL]: User{ID:1, Name:Alice} (changed: [])
// Latest: User{ID:1, Name:Alice Smith}
```

## License

MIT
