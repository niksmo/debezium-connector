package logger

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

type Logger struct {
	zerolog.Logger
}

func New(level string) Logger {
	setLevel(level)
	zl := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"},
	).With().Timestamp().Logger()
	return Logger{zl}
}

func setLevel(level string) {
	const op = "logger.setLevel"
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		fmt.Printf("%s: %s\n", op, "unknown level")
		os.Exit(1)
	}
	zerolog.SetGlobalLevel(lvl)
}

func (l Logger) WithOp(op string) Logger {
	return Logger{l.With().Str("op", op).Logger()}
}
