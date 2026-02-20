// Package mongodb provides a MongoDB implementation of the govers Repository interface.
// It uses the official MongoDB Go driver and stores snapshots in MongoDB collections.
// The schema is inspired by JaVers and uses 2 collections:
//   - gv_head_id — one document with the last CommitId
//   - gv_snapshots — domain object snapshots with embedded commit metadata
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/ralscha/govers/core"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	defaultSnapshotsCollectionName = "gv_snapshots"
	defaultHeadIDCollectionName    = "gv_head_id"

	// Field names for snapshots
	fieldGlobalIDKey           = "globalId_key"
	fieldGlobalIDEntity        = "globalId.entity"
	fieldGlobalIDValueObject   = "globalId.valueObject"
	fieldGlobalIDOwnerIDEntity = "globalId.ownerId.entity"
	fieldGlobalIDOwnerIDCdoID  = "globalId.ownerId.cdoId"
	fieldVersion               = "version"
	fieldChangedProperties     = "changedProperties"
	fieldCommitID              = "commitMetadata.id"
	fieldCommitAuthor          = "commitMetadata.author"
	fieldCommitDate            = "commitMetadata.commitDate"
	fieldCommitProperties      = "commitMetadata.properties"
)

// GlobalIDDocument represents the globalId subdocument in a snapshot.
type GlobalIDDocument struct {
	// For InstanceId: entity contains the type name
	Entity string `bson:"entity,omitempty"`
	// For InstanceId: cdoId contains the local id
	CdoID any `bson:"cdoId,omitempty"`
	// For ValueObjectId: valueObject contains the type name
	ValueObject string `bson:"valueObject,omitempty"`
	// For ValueObjectId: ownerId contains the owner's InstanceId info
	OwnerID *OwnerIDDocument `bson:"ownerId,omitempty"`
	// For ValueObjectId: fragment contains the path from owner
	Fragment string `bson:"fragment,omitempty"`
}

// OwnerIDDocument represents the ownerId subdocument for ValueObjectId.
type OwnerIDDocument struct {
	Entity string `bson:"entity"`
	CdoID  any    `bson:"cdoId"`
}

// CommitPropertyDocument represents a key-value property.
type CommitPropertyDocument struct {
	Key   string `bson:"key"`
	Value string `bson:"value"`
}

// CommitMetadataDocument represents the commitMetadata subdocument.
type CommitMetadataDocument struct {
	ID         float64                  `bson:"id"`
	Author     string                   `bson:"author"`
	CommitDate time.Time                `bson:"commitDate"`
	Properties []CommitPropertyDocument `bson:"properties,omitempty"`
}

// StateDocument represents the state subdocument in a snapshot.
type StateDocument struct {
	Properties            bson.M   `bson:"properties"`
	IgnoreOrderProperties []string `bson:"ignoreOrderProperties,omitempty"`
}

// SnapshotDocument represents a snapshot document in MongoDB.
type SnapshotDocument struct {
	GlobalIDKey       string                 `bson:"globalId_key"`
	GlobalID          GlobalIDDocument       `bson:"globalId"`
	State             StateDocument          `bson:"state"`
	ChangedProperties []string               `bson:"changedProperties,omitempty"`
	Type              string                 `bson:"type"`
	Version           int64                  `bson:"version"`
	CommitMetadata    CommitMetadataDocument `bson:"commitMetadata"`
}

// HeadIDDocument represents the head ID document.
type HeadIDDocument struct {
	ID string `bson:"id"`
}

// Repository provides a MongoDB implementation of core.Repository.
type Repository struct {
	client                  *mongo.Client
	database                *mongo.Database
	snapshotsCollectionName string
	headIDCollectionName    string
}

// RepositoryOption is a functional option for configuring the Repository.
type RepositoryOption func(*Repository)

// WithSnapshotsCollectionName sets a custom name for the snapshots collection.
func WithSnapshotsCollectionName(name string) RepositoryOption {
	return func(r *Repository) {
		r.snapshotsCollectionName = name
	}
}

// WithHeadIDCollectionName sets a custom name for the head ID collection.
func WithHeadIDCollectionName(name string) RepositoryOption {
	return func(r *Repository) {
		r.headIDCollectionName = name
	}
}

// New creates a new MongoDB repository with the given database.
func New(database *mongo.Database, opts ...RepositoryOption) *Repository {
	r := &Repository{
		database:                database,
		snapshotsCollectionName: defaultSnapshotsCollectionName,
		headIDCollectionName:    defaultHeadIDCollectionName,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewWithConnString creates a new MongoDB repository with the given connection string.
func NewWithConnString(ctx context.Context, connString, databaseName string, opts ...RepositoryOption) (*Repository, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(connString))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	r := &Repository{
		client:                  client,
		database:                client.Database(databaseName),
		snapshotsCollectionName: defaultSnapshotsCollectionName,
		headIDCollectionName:    defaultHeadIDCollectionName,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Close closes the database connection.
func (r *Repository) Close(ctx context.Context) error {
	if r.client != nil {
		return r.client.Disconnect(ctx)
	}
	return nil
}

func (r *Repository) snapshotsCollection() *mongo.Collection {
	return r.database.Collection(r.snapshotsCollectionName)
}

func (r *Repository) headIDCollection() *mongo.Collection {
	return r.database.Collection(r.headIDCollectionName)
}

// EnsureSchema creates the required indexes for the collections.
func (r *Repository) EnsureSchema(ctx context.Context) error {
	collection := r.snapshotsCollection()

	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: fieldGlobalIDKey, Value: 1}}},
		{Keys: bson.D{{Key: fieldGlobalIDEntity, Value: 1}}},
		{Keys: bson.D{{Key: fieldGlobalIDValueObject, Value: 1}}},
		{Keys: bson.D{{Key: fieldGlobalIDOwnerIDEntity, Value: 1}}},
		{Keys: bson.D{{Key: fieldGlobalIDOwnerIDCdoID, Value: 1}}},
		{Keys: bson.D{{Key: fieldChangedProperties, Value: 1}}},
		{Keys: bson.D{{Key: fieldCommitID, Value: 1}}},
		{Keys: bson.D{{Key: fieldCommitDate, Value: 1}}},
		{Keys: bson.D{{Key: fieldCommitAuthor, Value: 1}}},
		{Keys: bson.D{
			{Key: fieldCommitProperties + ".key", Value: 1},
			{Key: fieldCommitProperties + ".value", Value: 1},
		}},
	}

	_, err := collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	return nil
}

func commitIDToFloat(id core.CommitID) float64 {
	return float64(id.MajorID) + float64(id.MinorID)/100.0
}

func floatToCommitID(f float64) core.CommitID {
	majorID := int64(f)
	minorID := int((f - float64(majorID)) * 100)
	return core.CommitID{MajorID: majorID, MinorID: minorID}
}

// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
func (r *Repository) GetHeadID(ctx context.Context) (core.CommitID, error) {
	var headDoc HeadIDDocument
	err := r.headIDCollection().FindOne(ctx, bson.D{}).Decode(&headDoc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return core.CommitID{}, nil
	}
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to get head id: %w", err)
	}

	var majorID int64
	var minorID int
	_, err = fmt.Sscanf(headDoc.ID, "%d.%d", &majorID, &minorID)
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to parse head id: %w", err)
	}

	return core.CommitID{MajorID: majorID, MinorID: minorID}, nil
}

// Persist saves a commit and its snapshots to the repository.
func (r *Repository) Persist(ctx context.Context, commit core.Commit) error {
	for _, snapshot := range commit.Snapshots {
		doc := r.snapshotToDocument(snapshot)
		_, err := r.snapshotsCollection().InsertOne(ctx, doc)
		if err != nil {
			return fmt.Errorf("failed to insert snapshot: %w", err)
		}
	}

	if err := r.updateHeadID(ctx, commit.Metadata.ID); err != nil {
		return fmt.Errorf("failed to update head id: %w", err)
	}

	return nil
}

func (r *Repository) updateHeadID(ctx context.Context, commitID core.CommitID) error {
	headDoc := HeadIDDocument{ID: commitID.String()}
	opts := options.Replace().SetUpsert(true)

	_, err := r.headIDCollection().ReplaceOne(ctx, bson.D{}, headDoc, opts)
	if err != nil {
		return fmt.Errorf("failed to update head id: %w", err)
	}

	return nil
}

func (r *Repository) snapshotToDocument(snapshot core.Snapshot) SnapshotDocument {
	properties := make(bson.M)
	snapshot.State.ForEachProperty(func(name string, value any) {
		properties[name] = value
	})

	var ignoreOrderProperties []string
	for _, name := range snapshot.State.GetPropertyNames() {
		if snapshot.State.ShouldIgnoreOrder(name) {
			ignoreOrderProperties = append(ignoreOrderProperties, name)
		}
	}

	doc := SnapshotDocument{
		GlobalIDKey: snapshot.GlobalID.Value(),
		State: StateDocument{
			Properties:            properties,
			IgnoreOrderProperties: ignoreOrderProperties,
		},
		ChangedProperties: snapshot.ChangedProperties,
		Type:              string(snapshot.Type),
		Version:           snapshot.Version,
		CommitMetadata: CommitMetadataDocument{
			ID:         commitIDToFloat(snapshot.CommitMetadata.ID),
			Author:     snapshot.CommitMetadata.Author,
			CommitDate: snapshot.CommitMetadata.CommitDate,
		},
	}

	if len(snapshot.CommitMetadata.Properties) > 0 {
		props := make([]CommitPropertyDocument, 0, len(snapshot.CommitMetadata.Properties))
		for k, v := range snapshot.CommitMetadata.Properties {
			props = append(props, CommitPropertyDocument{Key: k, Value: v})
		}
		doc.CommitMetadata.Properties = props
	}

	switch gid := snapshot.GlobalID.(type) {
	case core.InstanceID:
		doc.GlobalID = GlobalIDDocument{
			Entity: gid.TypeName(),
			CdoID:  gid.CdoID(),
		}
	case core.ValueObjectID:
		doc.GlobalID = GlobalIDDocument{
			ValueObject: gid.TypeName(),
			Fragment:    gid.Fragment(),
			OwnerID: &OwnerIDDocument{
				Entity: gid.OwnerID().TypeName(),
				CdoID:  gid.OwnerID().CdoID(),
			},
		}
	}

	return doc
}

func (r *Repository) documentToSnapshot(doc SnapshotDocument) core.Snapshot {
	var globalID core.GlobalID
	if doc.GlobalID.Entity != "" {
		globalID = core.NewInstanceID(doc.GlobalID.Entity, doc.GlobalID.CdoID)
	} else if doc.GlobalID.ValueObject != "" && doc.GlobalID.OwnerID != nil {
		ownerID := core.NewInstanceID(doc.GlobalID.OwnerID.Entity, doc.GlobalID.OwnerID.CdoID)
		globalID = core.NewValueObjectID(doc.GlobalID.ValueObject, ownerID, doc.GlobalID.Fragment)
	}

	properties := make(map[string]string)
	for _, prop := range doc.CommitMetadata.Properties {
		properties[prop.Key] = prop.Value
	}

	// Convert BSON document back to SnapshotState
	var snapshotState core.SnapshotState
	if len(doc.State.Properties) > 0 {
		properties := make(map[string]any, len(doc.State.Properties))
		maps.Copy(properties, doc.State.Properties)
		snapshotState = core.NewSnapshotStateWithOptions(properties, doc.State.IgnoreOrderProperties)
	} else {
		snapshotState = core.EmptySnapshotState()
	}

	return core.Snapshot{
		GlobalID:          globalID,
		State:             snapshotState,
		ChangedProperties: doc.ChangedProperties,
		Type:              core.SnapshotType(doc.Type),
		Version:           doc.Version,
		CommitMetadata: core.CommitMetadata{
			ID:         floatToCommitID(doc.CommitMetadata.ID),
			Author:     doc.CommitMetadata.Author,
			CommitDate: doc.CommitMetadata.CommitDate,
			Properties: properties,
		},
	}
}

// GetLatestSnapshot returns the most recent snapshot for the given GlobalID.
// Returns nil if no snapshot exists for this GlobalID.
func (r *Repository) GetLatestSnapshot(ctx context.Context, globalID core.GlobalID) (*core.Snapshot, error) {
	filter := bson.D{{Key: fieldGlobalIDKey, Value: globalID.Value()}}
	opts := options.FindOne().SetSort(bson.D{{Key: fieldVersion, Value: -1}})

	var doc SnapshotDocument
	err := r.snapshotsCollection().FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	snapshot := r.documentToSnapshot(doc)
	return &snapshot, nil
}

// GetSnapshots returns snapshots matching the given query.
func (r *Repository) GetSnapshots(ctx context.Context, query core.Query) ([]core.Snapshot, error) {
	filter := bson.D{}

	switch query.Type {
	case core.QueryByInstanceID:
		if query.InstanceID != nil {
			filter = append(filter, bson.E{Key: fieldGlobalIDKey, Value: query.InstanceID.Value()})
		}
	case core.QueryByClass:
		filter = append(filter, bson.E{
			Key: "$or",
			Value: bson.A{
				bson.D{{Key: fieldGlobalIDEntity, Value: query.TypeName}},
				bson.D{{Key: fieldGlobalIDValueObject, Value: query.TypeName}},
			},
		})
	case core.QueryAny:
		// No filter for QueryAny
	}

	if query.Version > 0 {
		filter = append(filter, bson.E{Key: fieldVersion, Value: query.Version})
	}

	if query.Author != "" {
		filter = append(filter, bson.E{Key: fieldCommitAuthor, Value: query.Author})
	}

	if !query.CommitID.IsZero() {
		commitIDFloat := commitIDToFloat(query.CommitID)
		filter = append(filter, bson.E{
			Key: fieldCommitID,
			Value: bson.D{
				{Key: "$gte", Value: commitIDFloat - 0.005},
				{Key: "$lte", Value: commitIDFloat + 0.005},
			},
		})
	}

	if !query.FromDate.IsZero() {
		filter = append(filter, bson.E{Key: fieldCommitDate, Value: bson.D{{Key: "$gte", Value: query.FromDate}}})
	}

	if !query.ToDate.IsZero() {
		filter = append(filter, bson.E{Key: fieldCommitDate, Value: bson.D{{Key: "$lte", Value: query.ToDate}}})
	}

	if query.ChangedProperty != "" {
		filter = append(filter, bson.E{Key: fieldChangedProperties, Value: query.ChangedProperty})
	}

	opts := options.Find().SetSort(bson.D{
		{Key: fieldCommitDate, Value: -1},
		{Key: fieldVersion, Value: -1},
	})

	if query.Limit > 0 {
		opts.SetLimit(int64(query.Limit))
	}
	if query.Skip > 0 {
		opts.SetSkip(int64(query.Skip))
	}

	cursor, err := r.snapshotsCollection().Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var snapshots []core.Snapshot
	for cursor.Next(ctx) {
		var doc SnapshotDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode snapshot: %w", err)
		}
		snapshot := r.documentToSnapshot(doc)
		snapshots = append(snapshots, snapshot)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return snapshots, nil
}

// GetSnapshot returns a specific snapshot by GlobalID and version.
// Returns nil, nil if no such snapshot exists.
func (r *Repository) GetSnapshot(ctx context.Context, globalID core.GlobalID, version int64) (*core.Snapshot, error) {
	filter := bson.D{
		{Key: fieldGlobalIDKey, Value: globalID.Value()},
		{Key: fieldVersion, Value: version},
	}

	var doc SnapshotDocument
	err := r.snapshotsCollection().FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	snapshot := r.documentToSnapshot(doc)
	return &snapshot, nil
}

// Clear removes all data from the repository. Useful for testing.
func (r *Repository) Clear(ctx context.Context) error {
	if _, err := r.snapshotsCollection().DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("failed to clear snapshots: %w", err)
	}
	if _, err := r.headIDCollection().DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("failed to clear head id: %w", err)
	}
	return nil
}

// Ensure Repository implements core.Repository
var _ core.Repository = (*Repository)(nil)
