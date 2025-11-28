// Package main provides a demo application showcasing the govers library.
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
	if _, err := g.Commit(ctx, "admin", &user); err != nil {
		panic(err)
	}

	// Commit an update
	user.Name = "Alice Smith"
	if _, err := g.Commit(ctx, "admin", &user); err != nil {
		panic(err)
	}

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
