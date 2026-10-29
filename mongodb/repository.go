// Package mongodb provides a MongoDB implementation of the govers Repository interface.
// It uses the official MongoDB Go driver and stores snapshots in MongoDB collections.
// The schema is inspired by JaVers and uses 2 collections:
//   - gv_head_id: one document with the last CommitID
//   - gv_snapshots — domain object snapshots with embedded commit metadata
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ralscha/govers/core"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	defaultSnapshotsCollectionName = "gv_snapshots"
	defaultHeadIDCollectionName    = "gv_head_id"
	headDocumentKey                = "govers-head"

	// Field names for snapshots
	fieldGlobalIDKey           = "globalId_key"
	fieldGlobalIDEntity        = "globalId.entity"
	fieldGlobalIDValueObject   = "globalId.valueObject"
	fieldGlobalIDOwnerIDEntity = "globalId.ownerId.entity"
	fieldGlobalIDOwnerIDCdoID  = "globalId.ownerId.cdoId"
	fieldVersion               = "version"
	fieldSnapshotType          = "type"
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
	CdoID any `bson:"cdoId"`
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
	ID         string                   `bson:"id"`
	Author     string                   `bson:"author"`
	CommitDate time.Time                `bson:"commitDate"`
	Properties []CommitPropertyDocument `bson:"properties,omitempty"`
}

// StateDocument represents the state subdocument in a snapshot.
type StateDocument struct {
	Properties            bson.M   `bson:"properties"`
	IgnoreOrderProperties []string `bson:"ignoreOrderProperties,omitempty"`
	EntityProperties      []string `bson:"entityProperties,omitempty"`
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
		_ = client.Disconnect(context.Background())
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
		{
			Keys:    bson.D{{Key: fieldGlobalIDKey, Value: 1}, {Key: fieldVersion, Value: 1}},
			Options: options.Index().SetUnique(true),
		},
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

	return r.ensureCanonicalHead(ctx)
}

// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
func (r *Repository) GetHeadID(ctx context.Context) (core.CommitID, error) {
	var headDoc HeadIDDocument
	err := r.headIDCollection().FindOne(ctx, bson.D{{Key: "_id", Value: headDocumentKey}}).Decode(&headDoc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Compatibility with repositories created before the canonical head key.
		err = r.headIDCollection().FindOne(ctx, bson.D{}).Decode(&headDoc)
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return core.CommitID{}, nil
	}
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to get head id: %w", err)
	}

	headID, err := core.ParseCommitID(headDoc.ID)
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to parse head id: %w", err)
	}

	return headID, nil
}

// Persist saves a commit and its snapshots to the repository.
func (r *Repository) Persist(ctx context.Context, commit core.Commit) error {
	// Marshal the entire batch before writing, so an unsupported value cannot
	// leave an earlier snapshot behind. Allocate IDs locally for safe cleanup
	// even when a partial insert does not return its generated IDs.
	documents := make([]any, 0, len(commit.Snapshots))
	insertedIDs := make([]any, 0, len(commit.Snapshots))
	for _, snapshot := range commit.Snapshots {
		if snapshot.GlobalID == nil {
			return fmt.Errorf("snapshot has a nil global ID")
		}
		snapshotDocument, err := r.snapshotToDocument(snapshot)
		if err != nil {
			return fmt.Errorf("failed to marshal snapshot: %w", err)
		}
		id := bson.NewObjectID()
		document := struct {
			ID               bson.ObjectID `bson:"_id"`
			SnapshotDocument `bson:",inline"`
		}{ID: id, SnapshotDocument: snapshotDocument}
		encoded, err := bson.Marshal(document)
		if err != nil {
			return fmt.Errorf("failed to marshal snapshot: %w", err)
		}
		documents = append(documents, bson.Raw(encoded))
		insertedIDs = append(insertedIDs, id)
	}

	if err := r.ensureCanonicalHead(ctx); err != nil {
		return err
	}
	headID, err := r.GetHeadID(ctx)
	if err != nil {
		return err
	}
	expectedID := headID.Next()
	if commit.Metadata.ID != expectedID {
		return fmt.Errorf("%w: expected commit ID %s, got %s", core.ErrConcurrentCommit, expectedID, commit.Metadata.ID)
	}

	if len(documents) > 0 {
		_, err := r.snapshotsCollection().InsertMany(ctx, documents)
		if err != nil {
			persistErr := fmt.Errorf("failed to insert snapshot: %w", err)
			if mongo.IsDuplicateKeyError(err) {
				persistErr = fmt.Errorf("%w: snapshot version already exists: %w", core.ErrConcurrentCommit, err)
			}
			return errors.Join(persistErr, r.removeSnapshots(ctx, insertedIDs))
		}
	}

	if err := r.advanceHeadID(ctx, headID, commit.Metadata.ID); err != nil {
		return errors.Join(err, r.removeSnapshots(ctx, insertedIDs))
	}

	return nil
}

func (r *Repository) advanceHeadID(ctx context.Context, previousID, commitID core.CommitID) error {
	filter := bson.D{
		{Key: "_id", Value: headDocumentKey},
		{Key: "id", Value: previousID.String()},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "id", Value: commitID.String()}}}}
	result, err := r.headIDCollection().UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update head id: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("%w: repository head advanced after commit ID %s was selected", core.ErrConcurrentCommit, commitID)
	}

	return nil
}

func (r *Repository) ensureCanonicalHead(ctx context.Context) error {
	var canonical HeadIDDocument
	err := r.headIDCollection().FindOne(ctx, bson.D{{Key: "_id", Value: headDocumentKey}}).Decode(&canonical)
	if err == nil {
		return nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("failed to inspect head id: %w", err)
	}

	initialID := core.CommitID{}.String()
	var legacy HeadIDDocument
	err = r.headIDCollection().FindOne(ctx, bson.D{}).Decode(&legacy)
	if err == nil {
		initialID = legacy.ID
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("failed to inspect legacy head id: %w", err)
	}

	_, err = r.headIDCollection().InsertOne(ctx, bson.D{
		{Key: "_id", Value: headDocumentKey},
		{Key: "id", Value: initialID},
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("failed to initialize head id: %w", err)
	}
	return nil
}

func (r *Repository) removeSnapshots(ctx context.Context, ids []any) error {
	if len(ids) == 0 {
		return nil
	}
	// A failed request often has an expired context. Rollback must still run,
	// but keep it bounded if the database is unavailable.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := r.snapshotsCollection().DeleteMany(cleanupCtx, bson.D{{
		Key:   "_id",
		Value: bson.D{{Key: "$in", Value: ids}},
	}})
	if err != nil {
		return fmt.Errorf("failed to roll back snapshots: %w", err)
	}
	return nil
}

func (r *Repository) snapshotToDocument(snapshot core.Snapshot) (SnapshotDocument, error) {
	globalID, err := normalizeGlobalID(snapshot.GlobalID)
	if err != nil {
		return SnapshotDocument{}, err
	}
	snapshot.GlobalID = globalID
	// Use the same representation as the JSON repository. In particular BSON
	// otherwise lowercases nested struct fields and truncates time nanoseconds.
	encodedState, err := snapshot.State.MarshalJSON()
	if err != nil {
		return SnapshotDocument{}, err
	}
	var normalizedState core.SnapshotState
	if err := normalizedState.UnmarshalJSON(encodedState); err != nil {
		return SnapshotDocument{}, err
	}
	properties := make(bson.M)
	normalizedState.ForEachProperty(func(name string, value any) {
		properties[name] = value
	})

	doc := SnapshotDocument{
		GlobalIDKey: snapshot.GlobalID.Value(),
		State: StateDocument{
			Properties:            properties,
			IgnoreOrderProperties: snapshot.State.GetIgnoreOrderPropertyNames(),
			EntityProperties:      snapshot.State.GetEntityPropertyNames(),
		},
		ChangedProperties: snapshot.ChangedProperties,
		Type:              string(snapshot.Type),
		Version:           snapshot.Version,
		CommitMetadata: CommitMetadataDocument{
			ID:         snapshot.CommitMetadata.ID.String(),
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

	return doc, nil
}

func (r *Repository) documentToSnapshot(doc SnapshotDocument) (core.Snapshot, error) {
	var globalID core.GlobalID
	if doc.GlobalID.Entity != "" {
		globalID = restoreInstanceID(doc.GlobalID.Entity, doc.GlobalID.CdoID, doc.GlobalIDKey)
	} else if doc.GlobalID.ValueObject != "" && doc.GlobalID.OwnerID != nil {
		ownerKey := strings.TrimSuffix(doc.GlobalIDKey, "#"+doc.GlobalID.Fragment)
		ownerID := restoreInstanceID(doc.GlobalID.OwnerID.Entity, doc.GlobalID.OwnerID.CdoID, ownerKey)
		globalID = core.NewValueObjectID(doc.GlobalID.ValueObject, ownerID, doc.GlobalID.Fragment)
	}
	if globalID == nil {
		return core.Snapshot{}, fmt.Errorf("snapshot document has an invalid global ID")
	}

	properties := make(map[string]string)
	for _, prop := range doc.CommitMetadata.Properties {
		properties[prop.Key] = prop.Value
	}

	// Convert BSON document back to SnapshotState
	var snapshotState core.SnapshotState
	if len(doc.State.Properties) > 0 {
		properties := make(map[string]any, len(doc.State.Properties))
		for name, value := range doc.State.Properties {
			properties[name] = normalizeBSONValue(value)
		}
		snapshotState = core.NewSnapshotStateWithMetadata(properties, doc.State.IgnoreOrderProperties, doc.State.EntityProperties)
	} else {
		snapshotState = core.EmptySnapshotState()
	}

	commitID, err := core.ParseCommitID(doc.CommitMetadata.ID)
	if err != nil {
		return core.Snapshot{}, fmt.Errorf("failed to parse commit id: %w", err)
	}

	return core.Snapshot{
		GlobalID:          globalID,
		State:             snapshotState,
		ChangedProperties: doc.ChangedProperties,
		Type:              core.SnapshotType(doc.Type),
		Version:           doc.Version,
		CommitMetadata: core.CommitMetadata{
			ID:         commitID,
			Author:     doc.CommitMetadata.Author,
			CommitDate: doc.CommitMetadata.CommitDate,
			Properties: properties,
		},
	}, nil
}

func restoreInstanceID(typeName string, localID any, key string) core.InstanceID {
	id := core.NewInstanceID(typeName, normalizeBSONValue(localID))
	// BSON can change the type and formatting of IDs (for example byte slices
	// become bson.Binary). The stored key preserves the original identity.
	if key != "" && id.Value() != key {
		if stableLocalID, ok := strings.CutPrefix(key, typeName+"/"); ok {
			return core.NewInstanceID(typeName, stableLocalID)
		}
	}
	return id
}

// BSON's default interface decoder represents nested documents as ordered
// slices. Restore maps so persisted state compares with domain map values.
func normalizeBSONValue(value any) any {
	switch value := value.(type) {
	case bson.D:
		result := make(map[string]any, len(value))
		for _, entry := range value {
			result[entry.Key] = normalizeBSONValue(entry.Value)
		}
		return result
	case bson.M:
		result := make(map[string]any, len(value))
		for key, entry := range value {
			result[key] = normalizeBSONValue(entry)
		}
		return result
	case bson.A:
		result := make([]any, len(value))
		for index, entry := range value {
			result[index] = normalizeBSONValue(entry)
		}
		return result
	case bson.Binary:
		if value.Subtype == 0 {
			return value.Data
		}
	case bson.DateTime:
		return value.Time()
	}
	return value
}

// GetLatestSnapshot returns the most recent snapshot for the given GlobalID.
// Returns nil if no snapshot exists for this GlobalID.
func (r *Repository) GetLatestSnapshot(ctx context.Context, globalID core.GlobalID) (*core.Snapshot, error) {
	globalID, err := normalizeGlobalID(globalID)
	if err != nil {
		return nil, err
	}
	filter := bson.D{{Key: fieldGlobalIDKey, Value: globalID.Value()}}
	opts := options.FindOne().SetSort(bson.D{{Key: fieldVersion, Value: -1}})

	var doc SnapshotDocument
	err = r.snapshotsCollection().FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	snapshot, err := r.documentToSnapshot(doc)
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// GetSnapshots returns snapshots matching the given query.
func (r *Repository) GetSnapshots(ctx context.Context, query core.Query) ([]core.Snapshot, error) {
	filter, err := snapshotFilter(query)
	if err != nil {
		return nil, err
	}

	cursor, err := r.snapshotsCollection().Aggregate(ctx, snapshotPipeline(filter, query),
		options.Aggregate().SetCollation(&options.Collation{Locale: "simple"}))
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
		snapshot, err := r.documentToSnapshot(doc)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return snapshots, nil
}

func snapshotPipeline(filter bson.D, query core.Query) mongo.Pipeline {
	commitPart := func(index int) bson.D {
		return bson.D{{Key: "$toLong", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{
			bson.D{{Key: "$split", Value: bson.A{"$" + fieldCommitID, "."}}}, index,
		}}}}}
	}
	// Commit IDs are stored as strings. Sort their components numerically so
	// commits 10.0 and 9.0 remain correctly ordered when BSON dates tie.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$set", Value: bson.D{
			{Key: "_commitMajor", Value: commitPart(0)},
			{Key: "_commitMinor", Value: commitPart(1)},
		}}},
		{{Key: "$sort", Value: bson.D{
			{Key: fieldCommitDate, Value: -1},
			{Key: "_commitMajor", Value: -1},
			{Key: "_commitMinor", Value: -1},
			{Key: fieldVersion, Value: -1},
			{Key: fieldGlobalIDKey, Value: 1},
			{Key: "_id", Value: 1},
		}}},
	}
	if query.Skip > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$skip", Value: int64(query.Skip)}})
	}
	if query.Limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: int64(query.Limit)}})
	}
	return pipeline
}

func snapshotFilter(query core.Query) (bson.D, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
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
	if query.SnapshotType != "" {
		filter = append(filter, bson.E{Key: fieldSnapshotType, Value: string(query.SnapshotType)})
	}

	if query.Author != "" {
		filter = append(filter, bson.E{Key: fieldCommitAuthor, Value: query.Author})
	}

	if !query.CommitID.IsZero() {
		filter = append(filter, bson.E{Key: fieldCommitID, Value: query.CommitID.String()})
	}

	dateRange := bson.D{}
	if !query.FromDate.IsZero() {
		dateRange = append(dateRange, bson.E{Key: "$gte", Value: query.FromDate})
	}
	if !query.ToDate.IsZero() {
		dateRange = append(dateRange, bson.E{Key: "$lte", Value: query.ToDate})
	}
	if len(dateRange) > 0 {
		filter = append(filter, bson.E{Key: fieldCommitDate, Value: dateRange})
	}

	if query.ChangedProperty != "" {
		filter = append(filter, bson.E{Key: fieldChangedProperties, Value: query.ChangedProperty})
	}

	return filter, nil
}

// GetSnapshot returns a specific snapshot by GlobalID and version.
// Returns nil, nil if no such snapshot exists.
func (r *Repository) GetSnapshot(ctx context.Context, globalID core.GlobalID, version int64) (*core.Snapshot, error) {
	if version <= 0 {
		return nil, core.ErrInvalidVersion
	}
	globalID, err := normalizeGlobalID(globalID)
	if err != nil {
		return nil, err
	}
	filter := bson.D{
		{Key: fieldGlobalIDKey, Value: globalID.Value()},
		{Key: fieldVersion, Value: version},
	}

	var doc SnapshotDocument
	err = r.snapshotsCollection().FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	snapshot, err := r.documentToSnapshot(doc)
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func normalizeGlobalID(globalID core.GlobalID) (core.GlobalID, error) {
	switch id := globalID.(type) {
	case core.InstanceID, core.ValueObjectID:
		return globalID, nil
	case *core.InstanceID:
		if id != nil {
			return *id, nil
		}
	case *core.ValueObjectID:
		if id != nil {
			return *id, nil
		}
	}
	return nil, fmt.Errorf("invalid GlobalID: %T", globalID)
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
