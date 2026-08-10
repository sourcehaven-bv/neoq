package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestStripMigrateParams verifies that golang-migrate "x-" connection-string parameters are removed from the connection
// config (so they are never sent to Postgres as runtime parameters) and that a caller-supplied x-migrations-table value
// is accepted, defaulting to migrationsTable when absent.
func TestStripMigrateParams(t *testing.T) {
	const (
		appNameParam = "application_name"
		appNameValue = "neoq"
		sslModeParam = "sslmode"
		sslDisable   = "disable"
		customTable  = "custom_table"
	)

	tests := []struct {
		name          string
		runtimeParams map[string]string
		wantTable     string
		wantRemaining map[string]string
	}{
		{
			name:          "no migrate params defaults to neoq_schema_migrations",
			runtimeParams: map[string]string{appNameParam: appNameValue},
			wantTable:     migrationsTable,
			wantRemaining: map[string]string{appNameParam: appNameValue},
		},
		{
			name:          "custom x-migrations-table is accepted and stripped",
			runtimeParams: map[string]string{queryParamMigrationsTableName: customTable, appNameParam: appNameValue},
			wantTable:     customTable,
			wantRemaining: map[string]string{appNameParam: appNameValue},
		},
		{
			name:          "empty x-migrations-table falls back to default and is stripped",
			runtimeParams: map[string]string{queryParamMigrationsTableName: ""},
			wantTable:     migrationsTable,
			wantRemaining: map[string]string{},
		},
		{
			name:          "other x- migrate params are stripped",
			runtimeParams: map[string]string{"x-statement-timeout": "5000", sslModeParam: sslDisable},
			wantTable:     migrationsTable,
			wantRemaining: map[string]string{sslModeParam: sslDisable},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &pgx.ConnConfig{}
			cfg.RuntimeParams = tt.runtimeParams

			got := stripMigrateParams(cfg)
			if got != tt.wantTable {
				t.Errorf("stripMigrateParams returned table %q, want %q", got, tt.wantTable)
			}

			if len(cfg.RuntimeParams) != len(tt.wantRemaining) {
				t.Fatalf("remaining RuntimeParams = %v, want %v", cfg.RuntimeParams, tt.wantRemaining)
			}
			for k, want := range tt.wantRemaining {
				if cfg.RuntimeParams[k] != want {
					t.Errorf("remaining RuntimeParams[%q] = %q, want %q", k, cfg.RuntimeParams[k], want)
				}
			}
		})
	}
}
