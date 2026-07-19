// Package testsupport holds test-only helpers shared across internal
// packages (store, scheduler, worker). It's a regular (non-_test.go)
// package specifically so it can be imported from test files in multiple
// other packages — Go doesn't let _test.go files be imported elsewhere.
package testsupport

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/wiktor-cl/taskorbit/internal/migrate"
)

// NewTestPool starts a throwaway Postgres container, applies every
// migration, and returns a ready-to-use pool. Container and pool are both
// torn down automatically via t.Cleanup.
func NewTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("taskorbit"),
		tcpostgres.WithUsername("taskorbit"),
		tcpostgres.WithPassword("taskorbit"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := migrate.Apply(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	return pool
}
