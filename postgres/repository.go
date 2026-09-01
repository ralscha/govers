// Package postgres provides a PostgreSQL implementation of the govers Repository interface.
// It uses pgx/v5 for database connectivity and stores snapshots and commits in PostgreSQL tables.
// The schema is inspired by Javers and uses 4 tables: gv_global_id, gv_commit, gv_commit_property, gv_snapshot.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ralscha/govers/core"
)

// Repository provides a PostgreSQL implementation of core.Repository.
type Repository struct {
	pool *pgxpool.Pool
}

const snapshotSelectColumns = `
	g.local_id, g.type_name, g.fragment,
	owner.local_id, owner.type_name,
	s.state, s.changed_properties, s.type, s.version,
	c.commit_id::text, c.author, c.commit_date,
	COALESCE((
		SELECT jsonb_object_agg(p.property_name, p.property_value)
		FROM gv_commit_property p
		WHERE p.commit_fk = c.commit_pk
	), '{}'::jsonb)`

type rowScanner interface {
	Scan(dest ...any) error
}

// New creates a new PostgreSQL repository with the given connection pool.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// NewWithConnString creates a new PostgreSQL repository with the given connection string.
func NewWithConnString(ctx context.Context, connString string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}
	return &Repository{pool: pool}, nil
}

// Close closes the database connection pool.
func (r *Repository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// CreateSchema creates the required database tables if they don't exist.
// The schema uses 4 tables inspired by Javers:
//   - gv_global_id: domain object identifiers
//   - gv_commit: commit metadata
//   - gv_commit_property: commit properties (key-value pairs)
//   - gv_snapshot: domain object snapshots
func (r *Repository) CreateSchema(ctx context.Context) error {
	schema := `
		-- Global ID table: stores unique identifiers for domain objects
		CREATE TABLE IF NOT EXISTS gv_global_id (
			global_id_pk BIGSERIAL PRIMARY KEY,
			local_id VARCHAR(191) NOT NULL,
			fragment VARCHAR(200),
			type_name VARCHAR(200) NOT NULL,
			owner_id_fk BIGINT REFERENCES gv_global_id(global_id_pk)
		);

		CREATE INDEX IF NOT EXISTS gv_global_id_local_id_idx ON gv_global_id(local_id);
		CREATE INDEX IF NOT EXISTS gv_global_id_owner_id_fk_idx ON gv_global_id(owner_id_fk);
		CREATE UNIQUE INDEX IF NOT EXISTS gv_global_id_instance_uidx
			ON gv_global_id(type_name, local_id)
			WHERE fragment IS NULL AND owner_id_fk IS NULL;
		CREATE UNIQUE INDEX IF NOT EXISTS gv_global_id_value_object_uidx
			ON gv_global_id(type_name, owner_id_fk, fragment)
			WHERE fragment IS NOT NULL AND owner_id_fk IS NOT NULL;

		-- Commit table: stores commit metadata
		CREATE TABLE IF NOT EXISTS gv_commit (
			commit_pk BIGSERIAL PRIMARY KEY,
			author VARCHAR(200) NOT NULL,
			commit_date TIMESTAMP WITH TIME ZONE NOT NULL,
			commit_id DECIMAL(22,2) NOT NULL UNIQUE
		);

		CREATE INDEX IF NOT EXISTS gv_commit_commit_id_idx ON gv_commit(commit_id);

		-- Commit property table: stores key-value properties for commits
		CREATE TABLE IF NOT EXISTS gv_commit_property (
			commit_fk BIGINT NOT NULL REFERENCES gv_commit(commit_pk) ON DELETE CASCADE,
			property_name VARCHAR(191) NOT NULL,
			property_value VARCHAR(600),
			PRIMARY KEY (commit_fk, property_name)
		);

		CREATE INDEX IF NOT EXISTS gv_commit_property_commit_fk_idx ON gv_commit_property(commit_fk);
		CREATE INDEX IF NOT EXISTS gv_commit_property_property_name_property_value_idx ON gv_commit_property(property_name, property_value);

		-- Snapshot table: stores domain object snapshots
		CREATE TABLE IF NOT EXISTS gv_snapshot (
			snapshot_pk BIGSERIAL PRIMARY KEY,
			type VARCHAR(200) NOT NULL,
			version BIGINT NOT NULL,
			state TEXT NOT NULL,
			changed_properties TEXT,
			managed_type VARCHAR(200) NOT NULL,
			global_id_fk BIGINT NOT NULL REFERENCES gv_global_id(global_id_pk),
			commit_fk BIGINT NOT NULL REFERENCES gv_commit(commit_pk)
		);

		CREATE INDEX IF NOT EXISTS gv_snapshot_global_id_fk_idx ON gv_snapshot(global_id_fk);
		CREATE INDEX IF NOT EXISTS gv_snapshot_commit_fk_idx ON gv_snapshot(commit_fk);
		CREATE INDEX IF NOT EXISTS gv_snapshot_managed_type_idx ON gv_snapshot(managed_type);
		CREATE UNIQUE INDEX IF NOT EXISTS gv_snapshot_global_id_version_uidx
			ON gv_snapshot(global_id_fk, version);
	`

	_, err := r.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

func commitIDToDecimal(id core.CommitID) string {
	return fmt.Sprintf("%d.%02d", id.MajorID, id.MinorID)
}

func decimalToCommitID(decimal string) (core.CommitID, error) {
	if !strings.Contains(decimal, ".") {
		decimal += ".0"
	}
	return core.ParseCommitID(decimal)
}

// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
func (r *Repository) GetHeadID(ctx context.Context) (core.CommitID, error) {
	var commitIDDecimal string

	err := r.pool.QueryRow(ctx, `
		SELECT commit_id::text
		FROM gv_commit 
		ORDER BY commit_id DESC 
		LIMIT 1
	`).Scan(&commitIDDecimal)

	if errors.Is(err, pgx.ErrNoRows) {
		return core.CommitID{}, nil
	}
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to get head id: %w", err)
	}

	commitID, err := decimalToCommitID(commitIDDecimal)
	if err != nil {
		return core.CommitID{}, fmt.Errorf("failed to parse head id: %w", err)
	}

	return commitID, nil
}

func (r *Repository) getOrCreateGlobalID(ctx context.Context, tx pgx.Tx, globalID core.GlobalID) (int64, error) {
	var globalIDPk int64
	var localID, typeName string
	var fragment *string
	var ownerIDFk *int64

	switch gid := globalID.(type) {
	case core.InstanceID:
		localID = fmt.Sprintf("%v", gid.CdoID())
		typeName = gid.TypeName()
		fragment = nil
		ownerIDFk = nil
	case core.ValueObjectID:
		ownerPk, err := r.getOrCreateGlobalID(ctx, tx, gid.OwnerID())
		if err != nil {
			return 0, err
		}
		ownerIDFk = &ownerPk
		localID = fmt.Sprintf("%v", gid.OwnerID().CdoID())
		typeName = gid.TypeName()
		frag := gid.Fragment()
		fragment = &frag
	default:
		return 0, fmt.Errorf("unknown GlobalId type: %T", globalID)
	}

	var query string
	var args []any
	if fragment == nil {
		query = `SELECT global_id_pk FROM gv_global_id WHERE local_id = $1 AND type_name = $2 AND fragment IS NULL`
		args = []any{localID, typeName}
	} else {
		query = `SELECT global_id_pk FROM gv_global_id WHERE local_id = $1 AND type_name = $2 AND fragment = $3 AND owner_id_fk = $4`
		args = []any{localID, typeName, *fragment, *ownerIDFk}
	}

	err := tx.QueryRow(ctx, query, args...).Scan(&globalIDPk)
	if err == nil {
		return globalIDPk, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("failed to query global id: %w", err)
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO gv_global_id (local_id, type_name, fragment, owner_id_fk)
		VALUES ($1, $2, $3, $4)
		RETURNING global_id_pk
	`, localID, typeName, fragment, ownerIDFk).Scan(&globalIDPk)
	if err != nil {
		return 0, fmt.Errorf("failed to insert global id: %w", err)
	}

	return globalIDPk, nil
}

// Persist saves a commit and its snapshots to the repository.
func (r *Repository) Persist(ctx context.Context, commit core.Commit) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize commit-ID allocation across all repository instances sharing
	// this database. Govers selects the next ID before Persist, so verify that
	// the selected ID is still current while holding the transaction lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(119683, 1)`); err != nil {
		return fmt.Errorf("failed to lock commit sequence: %w", err)
	}
	var headIDDecimal string
	err = tx.QueryRow(ctx, `SELECT commit_id::text FROM gv_commit ORDER BY commit_id DESC LIMIT 1`).Scan(&headIDDecimal)
	var headID core.CommitID
	if err == nil {
		headID, err = decimalToCommitID(headIDDecimal)
		if err != nil {
			return fmt.Errorf("failed to parse head commit ID: %w", err)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed to read head commit ID: %w", err)
	}
	if expectedID := headID.Next(); commit.Metadata.ID != expectedID {
		return fmt.Errorf("%w: expected commit ID %s, got %s", core.ErrConcurrentCommit, expectedID, commit.Metadata.ID)
	}

	var commitPk int64
	commitIDDecimal := commitIDToDecimal(commit.Metadata.ID)
	err = tx.QueryRow(ctx, `
		INSERT INTO gv_commit (author, commit_date, commit_id)
		VALUES ($1, $2, $3)
		RETURNING commit_pk
	`, commit.Metadata.Author, commit.Metadata.CommitDate, commitIDDecimal).Scan(&commitPk)
	if err != nil {
		return fmt.Errorf("failed to insert commit: %w", err)
	}

	for key, value := range commit.Metadata.Properties {
		_, err = tx.Exec(ctx, `
			INSERT INTO gv_commit_property (commit_fk, property_name, property_value)
			VALUES ($1, $2, $3)
		`, commitPk, key, value)
		if err != nil {
			return fmt.Errorf("failed to insert commit property: %w", err)
		}
	}

	for _, snapshot := range commit.Snapshots {
		globalIDPk, err := r.getOrCreateGlobalID(ctx, tx, snapshot.GlobalID)
		if err != nil {
			return fmt.Errorf("failed to get/create global id: %w", err)
		}

		var changedPropsJSON []byte
		if len(snapshot.ChangedProperties) > 0 {
			changedPropsJSON, err = json.Marshal(snapshot.ChangedProperties)
			if err != nil {
				return fmt.Errorf("failed to marshal changed properties: %w", err)
			}
		}

		// Serialize SnapshotState to JSON
		stateJSON, err := json.Marshal(snapshot.State)
		if err != nil {
			return fmt.Errorf("failed to marshal state: %w", err)
		}

		managedType := snapshot.GlobalID.TypeName()

		_, err = tx.Exec(ctx, `
			INSERT INTO gv_snapshot (type, version, state, changed_properties, managed_type, global_id_fk, commit_fk)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, string(snapshot.Type), snapshot.Version, string(stateJSON), string(changedPropsJSON), managedType, globalIDPk, commitPk)
		if err != nil {
			return fmt.Errorf("failed to insert snapshot: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the most recent snapshot for the given GlobalID.
// Returns nil if no snapshot exists for this GlobalID.
func (r *Repository) GetLatestSnapshot(ctx context.Context, globalID core.GlobalID) (*core.Snapshot, error) {
	condition, args, err := globalIDCondition(globalID, 1)
	if err != nil {
		return nil, err
	}

	row := r.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		LEFT JOIN gv_global_id owner ON g.owner_id_fk = owner.global_id_pk
		WHERE %s
		ORDER BY s.version DESC
		LIMIT 1
	`, snapshotSelectColumns, condition), args...)

	snapshot, err := r.scanSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	return snapshot, nil
}

// GetSnapshots returns snapshots matching the given query.
func (r *Repository) GetSnapshots(ctx context.Context, query core.Query) ([]core.Snapshot, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	var conditions []string
	var args []any
	argNum := 1

	switch query.Type {
	case core.QueryByInstanceID:
		condition, identityArgs, err := globalIDCondition(*query.InstanceID, argNum)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
		args = append(args, identityArgs...)
		argNum += len(identityArgs)
	case core.QueryByClass:
		conditions = append(conditions, fmt.Sprintf("s.managed_type = $%d", argNum))
		args = append(args, query.TypeName)
		argNum++
	case core.QueryAny:
		// No filter for QueryAny
	}

	if query.Version > 0 {
		conditions = append(conditions, fmt.Sprintf("s.version = $%d", argNum))
		args = append(args, query.Version)
		argNum++
	}

	if query.Author != "" {
		conditions = append(conditions, fmt.Sprintf("c.author = $%d", argNum))
		args = append(args, query.Author)
		argNum++
	}

	if !query.CommitID.IsZero() {
		commitIDDecimal := commitIDToDecimal(query.CommitID)
		conditions = append(conditions, fmt.Sprintf("c.commit_id = $%d", argNum))
		args = append(args, commitIDDecimal)
		argNum++
	}

	if !query.FromDate.IsZero() {
		conditions = append(conditions, fmt.Sprintf("c.commit_date >= $%d", argNum))
		args = append(args, query.FromDate)
		argNum++
	}

	if !query.ToDate.IsZero() {
		conditions = append(conditions, fmt.Sprintf("c.commit_date <= $%d", argNum))
		args = append(args, query.ToDate)
		argNum++
	}

	if query.ChangedProperty != "" {
		conditions = append(conditions, fmt.Sprintf(`COALESCE(NULLIF(s.changed_properties, '')::jsonb, '[]'::jsonb) ? $%d`, argNum))
		args = append(args, query.ChangedProperty)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	sql := fmt.Sprintf(`
		SELECT %s
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		LEFT JOIN gv_global_id owner ON g.owner_id_fk = owner.global_id_pk
		%s
		ORDER BY c.commit_date DESC, c.commit_id DESC, s.version DESC, s.snapshot_pk DESC
	`, snapshotSelectColumns, whereClause)

	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	if query.Skip > 0 {
		sql += fmt.Sprintf(" OFFSET %d", query.Skip)
	}

	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []core.Snapshot
	for rows.Next() {
		snapshot, err := r.scanSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, *snapshot)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return snapshots, nil
}

// GetSnapshot returns a specific snapshot by GlobalID and version.
// Returns nil, nil if no such snapshot exists.
func (r *Repository) GetSnapshot(ctx context.Context, globalID core.GlobalID, version int64) (*core.Snapshot, error) {
	if version <= 0 {
		return nil, core.ErrInvalidVersion
	}
	condition, args, err := globalIDCondition(globalID, 1)
	if err != nil {
		return nil, err
	}
	args = append(args, version)
	versionArgNum := len(args)
	sql := fmt.Sprintf(`
		SELECT %s
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		LEFT JOIN gv_global_id owner ON g.owner_id_fk = owner.global_id_pk
		WHERE %s AND s.version = $%d
	`, snapshotSelectColumns, condition, versionArgNum)

	row := r.pool.QueryRow(ctx, sql, args...)

	snapshot, err := r.scanSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return snapshot, nil
}

func (r *Repository) scanSnapshot(row rowScanner) (*core.Snapshot, error) {
	var localID, typeName string
	var fragment, ownerLocalID, ownerTypeName *string
	var state, changedPropsJSON, propertiesJSON []byte
	var snapshotType string
	var version int64
	var commitIDDecimal string
	var author string
	var commitDate time.Time

	err := row.Scan(&localID, &typeName, &fragment, &ownerLocalID, &ownerTypeName,
		&state, &changedPropsJSON, &snapshotType, &version,
		&commitIDDecimal, &author, &commitDate, &propertiesJSON)
	if err != nil {
		return nil, err
	}

	return buildSnapshot(localID, typeName, fragment, ownerLocalID, ownerTypeName, state, changedPropsJSON,
		snapshotType, version, commitIDDecimal, author, commitDate, propertiesJSON)
}

func buildSnapshot(localID, typeName string, fragment, ownerLocalID, ownerTypeName *string,
	state, changedPropsJSON []byte, snapshotType string, version int64,
	commitIDDecimal string, author string, commitDate time.Time, propertiesJSON []byte) (*core.Snapshot, error) {
	var changedProperties []string
	if len(changedPropsJSON) > 0 {
		if err := json.Unmarshal(changedPropsJSON, &changedProperties); err != nil {
			return nil, fmt.Errorf("failed to unmarshal changed properties: %w", err)
		}
	}

	var snapshotState core.SnapshotState
	if len(state) > 0 {
		if err := json.Unmarshal(state, &snapshotState); err != nil {
			return nil, fmt.Errorf("failed to unmarshal state: %w", err)
		}
	} else {
		snapshotState = core.EmptySnapshotState()
	}

	commitID, err := decimalToCommitID(commitIDDecimal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse commit id: %w", err)
	}
	properties := make(map[string]string)
	if err := json.Unmarshal(propertiesJSON, &properties); err != nil {
		return nil, fmt.Errorf("failed to unmarshal commit properties: %w", err)
	}

	var globalID core.GlobalID
	if fragment != nil {
		if ownerLocalID == nil || ownerTypeName == nil {
			return nil, fmt.Errorf("value object snapshot is missing its owner")
		}
		ownerID := core.NewInstanceID(*ownerTypeName, *ownerLocalID)
		globalID = core.NewValueObjectID(typeName, ownerID, *fragment)
	} else {
		globalID = core.NewInstanceID(typeName, localID)
	}

	snapshot := core.Snapshot{
		GlobalID:          globalID,
		State:             snapshotState,
		ChangedProperties: changedProperties,
		Type:              core.SnapshotType(snapshotType),
		Version:           version,
		CommitMetadata: core.CommitMetadata{
			ID:         commitID,
			Author:     author,
			CommitDate: commitDate,
			Properties: properties,
		},
	}

	return &snapshot, nil
}

func globalIDCondition(globalID core.GlobalID, firstArg int) (string, []any, error) {
	switch gid := globalID.(type) {
	case core.InstanceID:
		condition := fmt.Sprintf("g.local_id = $%d AND g.type_name = $%d AND g.fragment IS NULL AND g.owner_id_fk IS NULL", firstArg, firstArg+1)
		return condition, []any{fmt.Sprintf("%v", gid.CdoID()), gid.TypeName()}, nil
	case core.ValueObjectID:
		condition := fmt.Sprintf("g.type_name = $%d AND g.fragment = $%d AND owner.local_id = $%d AND owner.type_name = $%d AND owner.fragment IS NULL AND owner.owner_id_fk IS NULL", firstArg, firstArg+1, firstArg+2, firstArg+3)
		return condition, []any{gid.TypeName(), gid.Fragment(), fmt.Sprintf("%v", gid.OwnerID().CdoID()), gid.OwnerID().TypeName()}, nil
	default:
		return "", nil, fmt.Errorf("unknown GlobalID type: %T", globalID)
	}
}

// Clear removes all data from the repository. Useful for testing.
func (r *Repository) Clear(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM gv_snapshot;
		DELETE FROM gv_commit_property;
		DELETE FROM gv_commit;
		DELETE FROM gv_global_id;
	`)
	return err
}

// Ensure Repository implements core.Repository
var _ core.Repository = (*Repository)(nil)
