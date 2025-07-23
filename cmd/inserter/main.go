package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/niksmo/kafka-connect-practice/pkg/logger"
)

var users = []user{
	{"John Doe", "john@example.com"},
	{"Jane Smith", "jane@example.com"},
	{"Alice Johnson", "alice@example.com"},
	{"Bob Brown", "bob@example.com"},
}

func main() {
	config := loadConfig()
	log := logger.New(config.logLevel)

	ctx, cancel := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM,
	)
	defer cancel()

	db, err := openDB(ctx, config.dsn)
	if err != nil {
		log.Panic().Err(err).Msg("failed to open database")
	}
	log.Info().Msg("database connected")
	defer func() {
		if err := db.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close database connection pool")
		}
		log.Info().Msg("database connection closed successfully")
	}()

	err = createUsers(ctx, log, db, users)
	if err != nil {
		log.Panic().Err(err).Msg("failed to create users")
	}
	log.Info().Msg("test users are prepared")

	log.Info().Msg("generating orders")
	err = runOrdersGenerator(ctx, log, db, config.generatorTick, len(users))
	if err != nil {
		log.Panic().Err(err).Msg("failed to generate orders")
	}
}

type config struct {
	logLevel      string
	dsn           string
	generatorTick time.Duration
}

func parseFlags() (dsn string) {
	dsnFlag := flag.String("dsn", "", "set user:password@host:port/dbname")
	flag.Parse()

	if *dsnFlag == "" {
		fmt.Println("-dsn flag is required")
		flag.CommandLine.Usage()
		os.Exit(1)
	}
	return "postgres://" + *dsnFlag
}

func loadConfig() config {
	dsn := parseFlags()
	return config{
		logLevel:      "info",
		dsn:           dsn,
		generatorTick: 5 * time.Second,
	}
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return db, nil
}

type user struct {
	name, email string
}

func createUsers(
	ctx context.Context, log logger.Logger, db *sql.DB, users []user,
) (err error) {
	const op = "createUsers"

	tx, errBegin := db.BeginTx(ctx, nil)
	if errBegin != nil {
		err = fmt.Errorf("%s: failed to begin tx: %w", op, errBegin)
		return
	}

	defer func() {
		if err != nil {
			if errRb := tx.Rollback(); errRb != nil {
				log.Error().Err(
					fmt.Errorf("%s: %w", op, errRb),
				).Msg("failed to rollback")
			}
			err = fmt.Errorf("%s: %w", op, err)
			return
		}

		if errCom := tx.Commit(); errCom != nil {
			err = fmt.Errorf("%s: %w", op, errCom)
		}
	}()

	stmt, errPrep := tx.PrepareContext(ctx,
		`
		INSERT INTO users (name, email) VALUES ($1, $2)
		ON CONFLICT DO NOTHING;`,
	)
	if errPrep != nil {
		err = fmt.Errorf("failed to prepare stmt: %w", errPrep)
		return
	}

	defer func() {
		if errClose := stmt.Close(); errClose != nil {
			log.Error().Err(
				fmt.Errorf("%s: %w", op, errClose),
			).Msg("failed to close stmt")
		}
	}()

	for _, user := range users {
		_, errExec := stmt.ExecContext(ctx, user.name, user.email)
		if err != nil {
			err = fmt.Errorf("failed to insert: %w", errExec)
			return
		}
	}

	return
}

func runOrdersGenerator(
	ctx context.Context,
	log logger.Logger,
	db *sql.DB,
	tick time.Duration,
	nUsers int,
) error {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := generateOrders(ctx, log, db, nUsers); err != nil {
				return err
			}
		}
	}
}

func generateOrders(
	ctx context.Context,
	log logger.Logger,
	db *sql.DB,
	nUsers int,
) (err error) {
	const (
		op = "generateOrders"
	)

	var output []string

	tx, errBegin := db.BeginTx(ctx, nil)
	if errBegin != nil {
		err = fmt.Errorf("%s: failed to begin tx: %w", op, errBegin)
		return
	}

	defer func() {
		if err != nil {
			if errRb := tx.Rollback(); errRb != nil {
				log.Error().Err(
					fmt.Errorf("%s: %w", op, errRb),
				).Msg("failed to rollback")
			}
			err = fmt.Errorf("%s: %w", op, err)
			return
		}

		if errCom := tx.Commit(); errCom != nil {
			err = fmt.Errorf("%s: %w", op, errCom)
			return
		}

		for _, s := range output {
			log.Info().Str("inserted", s).Send()
		}
	}()

	stmt, errPrep := tx.PrepareContext(ctx,
		`
		INSERT INTO orders (user_id, product_name, quantity)
		VALUES ($1, $2, $3);`,
	)
	if errPrep != nil {
		err = fmt.Errorf("failed to prepare stmt: %w", errPrep)
		return
	}

	defer func() {
		if errClose := stmt.Close(); errClose != nil {
			log.Error().Err(
				fmt.Errorf("%s: %w", op, errClose),
			).Msg("failed to close stmt")
		}
	}()

	for range nOrders() {
		userID := rand.IntN(nUsers) + 1
		pName := productName()
		q := rand.IntN(10) + 1

		_, errExec := stmt.ExecContext(ctx, userID, pName, q)
		if err != nil {
			err = fmt.Errorf("failed to insert: %w", errExec)
			return
		}

		output = append(
			output, fmt.Sprintf("userID=%d productName=%q quantity=%d",
				userID, pName, q),
		)
	}

	return
}

func nOrders() int {
	const (
		min = 4
		max = 10
	)

	return rand.IntN(max-min) + min + 1
}

func productName() string {
	runes := []rune("ABCDEFGHIJKLMOPQRSTUVWXYZ")
	return "Product " + string(runes[rand.IntN(len(runes))])
}
