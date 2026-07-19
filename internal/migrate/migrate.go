// Package migrate applies the embedded SQL migrations in filename order,
// tracking what's already been applied in a schema_migrations table. It's
// intentionally minimal (no down-migrations, no external CLI) — for a
// project this size, a tracked list of forward-only SQL files applied at
// startup is simpler to reason about than a full migration framework.
package migrate

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockID is an arbitrary constant identifying this project's
// migration advisory lock. Any int64 works; it just needs to not collide
// with a lock ID some other part of the system might use.
const migrationLockID = 727246001

// Apply runs every migration in migrations/ that isn't already recorded in
// schema_migrations, in filename order, each in its own transaction.
//
// All four of this project's processes (scheduler, two workers, api) call
// Apply on startup, so this takes a session-scoped Postgres advisory lock
// on one dedicated connection for the whole operation first. Without it,
// concurrent "CREATE TABLE IF NOT EXISTS" statements from multiple
// processes racing on a fresh database can collide on Postgres's own
// catalog (a unique index on pg_type covering each table's implicit row
// type) — caught by actually running `docker compose up`, not by
// reasoning about the SQL in isolation.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migration lock: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(migrationLockID)); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, int64(migrationLockID))
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		applied, err := isApplied(ctx, conn, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		contents, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(contents)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}

	return nil
}

func isApplied(ctx context.Context, conn *pgxpool.Conn, name string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, name,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check migration %s applied: %w", name, err)
	}
	return exists, nil
}
