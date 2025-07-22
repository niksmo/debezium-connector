package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/niksmo/kafka-connect-practice/pkg/logger"
)

const (
	storagePathFlag   = "s"
	migrationPathFlag = "m"
)

type MigrationLogger struct {
	logger  logger.Logger
	verbose bool
}

func NewMigrationLogger() *MigrationLogger {

	return &MigrationLogger{logger.New("debug"), true}
}

func (ml *MigrationLogger) Printf(format string, v ...any) {
	ml.logger.Info().Msgf(format, v...)
}

func (ml *MigrationLogger) Verbose() bool {
	return ml.verbose
}

func main() {
	logger := NewMigrationLogger()
	dsn, mPath := getFlagsValues()
	validateFlags(logger, dsn, mPath)
	makeMigrations(logger, dsn, mPath)
}

func getFlagsValues() (dsn, mPath string) {
	storagePath := flag.String("dsn", "", "set user:password@host:port")
	migrationsPath := flag.String("m", "", "set migration path")
	flag.Parse()
	return *storagePath, *migrationsPath
}

func validateFlags(logger *MigrationLogger, storagePath, migrationsPath string) {
	var errs []error

	if storagePath == "" {
		errs = append(errs, fmt.Errorf("-%s flag: required", "dsn"))
	}

	if migrationsPath == "" {
		errs = append(errs, fmt.Errorf("-%s flag: required", "m"))
	}

	if len(errs) != 0 {
		logger.logger.Error().Errs("errors", errs).Send()
		flag.CommandLine.Usage()
		os.Exit(1)
	}
}

func makeMigrations(
	logger *MigrationLogger, storagePath, migrationsPath string,
) {
	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		fmt.Sprintf("pgx5://%s/customers", storagePath),
	)
	if err != nil {
		logger.logger.Fatal().Err(err).Send()
	}

	m.Log = NewMigrationLogger()

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			m.Log.Printf("no migrations to apply")
			return
		}
		logger.logger.Fatal().Err(err)
	}

	m.Log.Printf("migration applied\n")
}
