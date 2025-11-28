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
	`

	_, err := r.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

func commitIDToDecimal(id core.CommitID) float64 {
	return float64(id.MajorID) + float64(id.MinorID)/100.0
}

func decimalToCommitID(decimal float64) core.CommitID {
	majorID := int64(decimal)
	minorID := int((decimal - float64(majorID)) * 100)
	return core.CommitID{MajorID: majorID, MinorID: minorID}
}

// GetHeadID returns the latest CommitID, or zero CommitID if no commits exist.
func (r *Repository) GetHeadID(ctx context.Context) (core.CommitID, error) {
	var commitIDDecimal float64

	err := r.pool.QueryRow(ctx, `
		SELECT commit_id 
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

	return decimalToCommitID(commitIDDecimal), nil
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
		query = `SELECT global_id_pk FROM gv_global_id WHERE local_id = $1 AND type_name = $2 AND fragment = $3`
		args = []any{localID, typeName, *fragment}
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
	localID, typeName, fragment := parseGlobalIDParts(globalID)

	var globalIDQuery string
	var globalIDArgs []any
	if fragment == "" {
		globalIDQuery = `SELECT global_id_pk FROM gv_global_id WHERE local_id = $1 AND type_name = $2 AND fragment IS NULL`
		globalIDArgs = []any{localID, typeName}
	} else {
		globalIDQuery = `SELECT global_id_pk FROM gv_global_id WHERE local_id = $1 AND type_name = $2 AND fragment = $3`
		globalIDArgs = []any{localID, typeName, fragment}
	}

	row := r.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT g.local_id, g.type_name, g.fragment, g.owner_id_fk,
		       s.state, s.changed_properties, s.type, s.version,
		       c.commit_id, c.author, c.commit_date
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		WHERE s.global_id_fk = (%s)
		ORDER BY s.version DESC
		LIMIT 1
	`, globalIDQuery), globalIDArgs...)

	snapshot, err := r.scanSnapshot(ctx, row)
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
	var conditions []string
	var args []any
	argNum := 1

	switch query.Type {
	case core.QueryByInstanceID:
		if query.InstanceID != nil {
			localID := fmt.Sprintf("%v", query.InstanceID.CdoID())
			conditions = append(conditions, fmt.Sprintf("g.local_id = $%d AND g.type_name = $%d AND g.fragment IS NULL", argNum, argNum+1))
			args = append(args, localID, query.InstanceID.TypeName())
			argNum += 2
		}
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
		conditions = append(conditions, fmt.Sprintf("s.changed_properties LIKE $%d", argNum))
		args = append(args, "%\""+query.ChangedProperty+"\"%")
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	sql := fmt.Sprintf(`
		SELECT g.local_id, g.type_name, g.fragment, g.owner_id_fk,
		       s.state, s.changed_properties, s.type, s.version,
		       c.commit_id, c.author, c.commit_date
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		%s
		ORDER BY c.commit_date DESC, s.version DESC
	`, whereClause)

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
		snapshot, err := r.scanSnapshotFromRows(ctx, rows)
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
	localID, typeName, fragment := parseGlobalIDParts(globalID)

	var globalIDQuery string
	var args []any
	if fragment == "" {
		globalIDQuery = `g.local_id = $1 AND g.type_name = $2 AND g.fragment IS NULL`
		args = []any{localID, typeName, version}
	} else {
		globalIDQuery = `g.local_id = $1 AND g.type_name = $2 AND g.fragment = $3`
		args = []any{localID, typeName, fragment, version}
	}

	versionArgNum := len(args)
	sql := fmt.Sprintf(`
		SELECT g.local_id, g.type_name, g.fragment, g.owner_id_fk,
		       s.state, s.changed_properties, s.type, s.version,
		       c.commit_id, c.author, c.commit_date
		FROM gv_snapshot s
		JOIN gv_global_id g ON s.global_id_fk = g.global_id_pk
		JOIN gv_commit c ON s.commit_fk = c.commit_pk
		WHERE %s AND s.version = $%d
	`, globalIDQuery, versionArgNum)

	row := r.pool.QueryRow(ctx, sql, args...)

	snapshot, err := r.scanSnapshot(ctx, row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return snapshot, nil
}

func (r *Repository) scanSnapshot(ctx context.Context, row pgx.Row) (*core.Snapshot, error) {
	var localID, typeName string
	var fragment *string
	var ownerIDFk *int64
	var state, changedPropsJSON []byte
	var snapshotType string
	var version int64
	var commitIDDecimal float64
	var author string
	var commitDate time.Time

	err := row.Scan(&localID, &typeName, &fragment, &ownerIDFk,
		&state, &changedPropsJSON, &snapshotType, &version,
		&commitIDDecimal, &author, &commitDate)
	if err != nil {
		return nil, err
	}

	return r.buildSnapshot(ctx, localID, typeName, fragment, ownerIDFk, state, changedPropsJSON,
		snapshotType, version, commitIDDecimal, author, commitDate)
}

func (r *Repository) scanSnapshotFromRows(ctx context.Context, rows pgx.Rows) (*core.Snapshot, error) {
	var localID, typeName string
	var fragment *string
	var ownerIDFk *int64
	var state, changedPropsJSON []byte
	var snapshotType string
	var version int64
	var commitIDDecimal float64
	var author string
	var commitDate time.Time

	err := rows.Scan(&localID, &typeName, &fragment, &ownerIDFk,
		&state, &changedPropsJSON, &snapshotType, &version,
		&commitIDDecimal, &author, &commitDate)
	if err != nil {
		return nil, err
	}

	return r.buildSnapshot(ctx, localID, typeName, fragment, ownerIDFk, state, changedPropsJSON,
		snapshotType, version, commitIDDecimal, author, commitDate)
}

func (r *Repository) buildSnapshot(ctx context.Context, localID, typeName string, fragment *string, ownerIDFk *int64,
	state, changedPropsJSON []byte, snapshotType string, version int64,
	commitIDDecimal float64, author string, commitDate time.Time) (*core.Snapshot, error) {
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

	commitID := decimalToCommitID(commitIDDecimal)
	properties, err := r.loadCommitProperties(ctx, commitIDDecimal)
	if err != nil {
		return nil, fmt.Errorf("failed to load commit properties: %w", err)
	}

	var globalID core.GlobalID
	if fragment != nil && ownerIDFk != nil {
		ownerLocalID, ownerTypeName, err := r.getGlobalIDInfo(ctx, *ownerIDFk)
		if err != nil {
			return nil, fmt.Errorf("failed to get owner info: %w", err)
		}
		ownerID := core.NewInstanceID(ownerTypeName, ownerLocalID)
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

func (r *Repository) loadCommitProperties(ctx context.Context, commitIDDecimal float64) (map[string]string, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT p.property_name, p.property_value
		FROM gv_commit_property p
		JOIN gv_commit c ON p.commit_fk = c.commit_pk
		WHERE c.commit_id = $1
	`, commitIDDecimal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	properties := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		properties[name] = value
	}

	return properties, rows.Err()
}

func (r *Repository) getGlobalIDInfo(ctx context.Context, globalIDPk int64) (string, string, error) {
	var localID, typeName string
	err := r.pool.QueryRow(ctx, `
		SELECT local_id, type_name FROM gv_global_id WHERE global_id_pk = $1
	`, globalIDPk).Scan(&localID, &typeName)
	return localID, typeName, err
}

func parseGlobalIDParts(globalID core.GlobalID) (localID, typeName, fragment string) {
	switch gid := globalID.(type) {
	case core.InstanceID:
		return fmt.Sprintf("%v", gid.CdoID()), gid.TypeName(), ""
	case core.ValueObjectID:
		return fmt.Sprintf("%v", gid.OwnerID().CdoID()), gid.TypeName(), gid.Fragment()
	default:
		return "", "", ""
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
