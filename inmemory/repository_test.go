package inmemory_test

import (
	"context"
	"slices"
	"testing"

	"github.com/ralscha/govers/core"
	"github.com/ralscha/govers/inmemory"
)

type Employee struct {
	ID        int    `govers:"id"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Salary    int    `json:"salary"`
}

func TestBasicCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{
		ID:        1,
		FirstName: "John",
		LastName:  "Doe",
		Salary:    50000,
	}

	commit, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	if len(commit.Snapshots) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(commit.Snapshots))
	}

	snapshot := commit.Snapshots[0]
	if snapshot.Type != core.Initial {
		t.Errorf("Expected INITIAL snapshot, got %s", snapshot.Type)
	}

	if snapshot.Version != 1 {
		t.Errorf("Expected version 1, got %d", snapshot.Version)
	}

	if snapshot.GlobalID.TypeName() != "Employee" {
		t.Errorf("Expected type name 'Employee', got '%s'", snapshot.GlobalID.TypeName())
	}

	if snapshot.State.GetPropertyValue("firstName") != "John" {
		t.Errorf("Expected firstName 'John', got '%v'", snapshot.State.GetPropertyValue("firstName"))
	}

	if len(commit.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(commit.Changes))
	}

	change := commit.Changes[0]
	if change.GetChangeType() != core.NewObjectChange {
		t.Errorf("Expected NewObject change, got %s", change.GetChangeType())
	}
}

func TestUpdateCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{
		ID:        1,
		FirstName: "John",
		LastName:  "Doe",
		Salary:    50000,
	}

	_, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	emp.Salary = 60000

	commit, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	snapshot := commit.Snapshots[0]
	if snapshot.Type != core.Update {
		t.Errorf("Expected UPDATE snapshot, got %s", snapshot.Type)
	}

	if snapshot.Version != 2 {
		t.Errorf("Expected version 2, got %d", snapshot.Version)
	}

	if len(snapshot.ChangedProperties) != 1 {
		t.Errorf("Expected 1 changed property, got %d", len(snapshot.ChangedProperties))
	}

	if snapshot.ChangedProperties[0] != "salary" {
		t.Errorf("Expected 'salary' changed, got '%s'", snapshot.ChangedProperties[0])
	}

	if len(commit.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(commit.Changes))
	}

	valueChange, ok := commit.Changes[0].(core.ValueChange)
	if !ok {
		t.Fatalf("Expected ValueChange, got %T", commit.Changes[0])
	}

	if valueChange.PropertyName != "salary" {
		t.Errorf("Expected property 'salary', got '%s'", valueChange.PropertyName)
	}

	if valueChange.Left != 50000 {
		t.Errorf("Expected left value 50000, got %v (type %T)", valueChange.Left, valueChange.Left)
	}

	if valueChange.Right != 60000 {
		t.Errorf("Expected right value 60000, got %v (type %T)", valueChange.Right, valueChange.Right)
	}
}

func TestNoChangeCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{
		ID:        1,
		FirstName: "John",
		LastName:  "Doe",
		Salary:    50000,
	}

	_, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	commit, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit != nil {
		t.Error("Expected nil commit when no changes")
	}
}

func TestFindSnapshots(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "user1", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 55000
	if _, err := g.Commit(ctx, "user2", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 60000
	if _, err := g.Commit(ctx, "user1", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	query := core.ByInstanceIDQuery("Employee", 1).Build()
	snapshots, err := g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 3 {
		t.Errorf("Expected 3 snapshots, got %d", len(snapshots))
	}

	query = core.ByInstanceIDQuery("Employee", 1).Limit(2).Build()
	snapshots, err = g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots (limited), got %d", len(snapshots))
	}

	query = core.ByInstanceIDQuery("Employee", 1).ByAuthor("user1").Build()
	snapshots, err = g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots by user1, got %d", len(snapshots))
	}
}

func TestDeleteCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	_, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	commit, err := g.Delete(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	snapshot := commit.Snapshots[0]
	if snapshot.Type != core.Terminal {
		t.Errorf("Expected TERMINAL snapshot, got %s", snapshot.Type)
	}

	if len(commit.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(commit.Changes))
	}

	if commit.Changes[0].GetChangeType() != core.ObjectRemovedChange {
		t.Errorf("Expected ObjectRemoved change, got %s", commit.Changes[0].GetChangeType())
	}
}

type Address struct {
	ID     int    `govers:"id"`
	Street string `json:"street"`
	City   string `json:"city"`
}

type PersonWithIgnore struct {
	ID           int    `govers:"id"`
	Name         string `json:"name"`
	InternalNote string `govers:"ignore"`
}

type PersonWithEntity struct {
	ID      int      `govers:"id"`
	Name    string   `json:"name"`
	Address *Address `govers:"entity" json:"address"`
}

type PersonWithIgnoreOrder struct {
	ID   int      `govers:"id"`
	Name string   `json:"name"`
	Tags []string `govers:"ignoreOrder" json:"tags"`
}

func TestIgnoreTagCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	person := PersonWithIgnore{
		ID:           1,
		Name:         "John Doe",
		InternalNote: "Secret note",
	}

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	snapshot := commit.Snapshots[0]

	if snapshot.State.GetPropertyValue("InternalNote") != nil {
		t.Errorf("Expected InternalNote to be ignored, but got '%v'", snapshot.State.GetPropertyValue("InternalNote"))
	}

	if snapshot.State.GetPropertyValue("name") != "John Doe" {
		t.Errorf("Expected name 'John Doe', got '%v'", snapshot.State.GetPropertyValue("name"))
	}
}

func TestIgnoreTagNoChangeWhenOnlyIgnoredFieldChanges(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	person := PersonWithIgnore{
		ID:           1,
		Name:         "John Doe",
		InternalNote: "Note 1",
	}

	_, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	person.InternalNote = "Note 2 - different!"

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit != nil {
		t.Error("Expected nil commit when only ignored field changes")
	}
}

func TestEntityTagCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	address := &Address{
		ID:     100,
		Street: "123 Main St",
		City:   "Boston",
	}

	person := PersonWithEntity{
		ID:      1,
		Name:    "John Doe",
		Address: address,
	}

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	snapshot := commit.Snapshots[0]

	addressValue := snapshot.State.GetPropertyValue("address")
	if addressValue == nil {
		t.Fatal("Expected address to be present")
	}

	addressStr, ok := addressValue.(string)
	if !ok {
		t.Fatalf("Expected address to be dehydrated to a string, got %T", addressValue)
	}

	expectedGlobalID := core.NewInstanceID("Address", 100).Value()
	if addressStr != expectedGlobalID {
		t.Errorf("Expected address GlobalId '%s', got '%s'", expectedGlobalID, addressStr)
	}
}

func TestEntityTagChangeDetection(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	address1 := &Address{ID: 100, Street: "123 Main St", City: "Boston"}
	person := PersonWithEntity{ID: 1, Name: "John", Address: address1}

	_, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	address2 := &Address{ID: 200, Street: "456 Oak Ave", City: "Chicago"}
	person.Address = address2

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	snapshot := commit.Snapshots[0]

	found := false
	for _, prop := range snapshot.ChangedProperties {
		if prop == "address" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'address' in changed properties, got %v", snapshot.ChangedProperties)
	}
}

func TestIgnoreOrderTagCommit(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	person := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John Doe",
		Tags: []string{"developer", "go", "backend"},
	}

	_, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	person.Tags = []string{"backend", "developer", "go"}

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit != nil {
		t.Error("Expected nil commit when only slice order changes with ignoreOrder tag")
	}
}

func TestIgnoreOrderTagWithDifferentElements(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	person := PersonWithIgnoreOrder{
		ID:   1,
		Name: "John Doe",
		Tags: []string{"developer", "go", "backend"},
	}

	_, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	person.Tags = []string{"developer", "python", "backend"}

	commit, err := g.Commit(ctx, "test-user", person)
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil when slice elements change")
	}

	snapshot := commit.Snapshots[0]

	found := false
	for _, prop := range snapshot.ChangedProperties {
		if prop == "tags" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'tags' in changed properties, got %v", snapshot.ChangedProperties)
	}
}

type EmployeeWithMap struct {
	ID       int               `govers:"id"`
	Name     string            `json:"name"`
	Metadata map[string]string `json:"metadata"`
}

func TestMapChangeDetection(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := EmployeeWithMap{
		ID:   1,
		Name: "John Doe",
		Metadata: map[string]string{
			"department": "engineering",
			"level":      "senior",
		},
	}

	_, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit initial: %v", err)
	}

	emp.Metadata = map[string]string{
		"department": "engineering",
		"level":      "principal",
		"location":   "remote",
	}

	commit, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil when map changes")
	}

	found := slices.Contains(commit.Snapshots[0].ChangedProperties, "metadata")
	if !found {
		t.Errorf("Expected 'metadata' in changed properties")
	}
}

func TestDeleteNonExistentObject(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}

	_, err := g.Delete(ctx, "test-user", emp)
	if err == nil {
		t.Error("Expected error when deleting non-existent object")
	}
}

func TestDeleteAlreadyDeletedObject(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}

	_, err := g.Commit(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	_, err = g.Delete(ctx, "test-user", emp)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, err = g.Delete(ctx, "test-user", emp)
	if err == nil {
		t.Error("Expected error when deleting already deleted object")
	}
}

func TestGetLatestSnapshot(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	snapshot, err := g.GetLatestSnapshot(ctx, "Employee", 1)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}
	if snapshot != nil {
		t.Error("Expected nil snapshot initially")
	}

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 60000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	snapshot, err = g.GetLatestSnapshot(ctx, "Employee", 1)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Expected snapshot, got nil")
	}

	if snapshot.Version != 2 {
		t.Errorf("Expected version 2, got %d", snapshot.Version)
	}
}

func TestGetSpecificSnapshot(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 60000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 70000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	snapshot, err := g.GetSnapshot(ctx, "Employee", 1, 2)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Expected snapshot, got nil")
	}

	if snapshot.Version != 2 {
		t.Errorf("Expected version 2, got %d", snapshot.Version)
	}

	snapshot, err = g.GetSnapshot(ctx, "Employee", 1, 99)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if snapshot != nil {
		t.Error("Expected nil for non-existent version")
	}
}

func TestQueryByClass(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp1 := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	emp2 := Employee{ID: 2, FirstName: "Jane", LastName: "Smith", Salary: 60000}
	emp3 := Employee{ID: 3, FirstName: "Bob", LastName: "Johnson", Salary: 55000}

	if _, err := g.Commit(ctx, "test-user", emp1); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if _, err := g.Commit(ctx, "test-user", emp2); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if _, err := g.Commit(ctx, "test-user", emp3); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	query := core.ByClassQuery("Employee").Build()
	snapshots, err := g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 3 {
		t.Errorf("Expected 3 snapshots, got %d", len(snapshots))
	}
}

func TestQueryWithSkip(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 55000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 60000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 65000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	query := core.ByInstanceIDQuery("Employee", 1).Skip(2).Build()
	snapshots, err := g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots after skipping 2, got %d", len(snapshots))
	}

	query = core.ByInstanceIDQuery("Employee", 1).Skip(1).Limit(2).Build()
	snapshots, err = g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots (skip 1, limit 2), got %d", len(snapshots))
	}
}

func TestQueryWithChangedProperty(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.Salary = 60000
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	emp.FirstName = "Johnny"
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	query := core.ByInstanceIDQuery("Employee", 1).WithChangedProperty("salary").Build()
	snapshots, err := g.FindSnapshots(ctx, query)
	if err != nil {
		t.Fatalf("Failed to find snapshots: %v", err)
	}

	if len(snapshots) != 1 {
		t.Errorf("Expected 1 snapshot with salary change, got %d", len(snapshots))
	}
}

func TestClear(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}
	if _, err := g.Commit(ctx, "test-user", emp); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if repo.Count() != 1 {
		t.Errorf("Expected 1 snapshot, got %d", repo.Count())
	}

	repo.Clear()

	if repo.Count() != 0 {
		t.Errorf("Expected 0 snapshots after clear, got %d", repo.Count())
	}

	headID, _ := repo.GetHeadID(ctx)
	if !headID.IsZero() {
		t.Error("Expected zero head id after clear")
	}
}

func TestCommitWithProperties(t *testing.T) {
	ctx := context.Background()

	repo := inmemory.New()
	g := core.New(core.WithRepository(repo))

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}

	properties := map[string]string{
		"ticket": "JIRA-123",
		"reason": "annual review",
	}

	commit, err := g.CommitWithProperties(ctx, "test-user", emp, properties)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if commit == nil {
		t.Fatal("Commit should not be nil")
	}

	if commit.Metadata.Properties["ticket"] != "JIRA-123" {
		t.Errorf("Expected ticket 'JIRA-123', got '%s'", commit.Metadata.Properties["ticket"])
	}

	if commit.Metadata.Properties["reason"] != "annual review" {
		t.Errorf("Expected reason 'annual review', got '%s'", commit.Metadata.Properties["reason"])
	}
}

func TestNoRepositoryConfigured(t *testing.T) {
	ctx := context.Background()

	g := core.New()

	emp := Employee{ID: 1, FirstName: "John", LastName: "Doe", Salary: 50000}

	_, err := g.Commit(ctx, "test-user", emp)
	if err == nil {
		t.Error("Expected error when no repository configured")
	}

	_, err = g.Delete(ctx, "test-user", emp)
	if err == nil {
		t.Error("Expected error when no repository configured")
	}

	_, err = g.FindSnapshots(ctx, core.ByInstanceIDQuery("Employee", 1).Build())
	if err == nil {
		t.Error("Expected error when no repository configured")
	}

	_, err = g.GetLatestSnapshot(ctx, "Employee", 1)
	if err == nil {
		t.Error("Expected error when no repository configured")
	}

	_, err = g.GetSnapshot(ctx, "Employee", 1, 1)
	if err == nil {
		t.Error("Expected error when no repository configured")
	}
}
