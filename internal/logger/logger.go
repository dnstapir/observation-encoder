package logger

/* Implements shared.LoggerIF */

import (
	"fmt"
	"log/slog"
	"os"
)

type logger struct {
	logger *slog.Logger
	conf   Conf
}

type Conf struct {
	Debug bool
}

func Create(conf Conf) (*logger, error) {
	newLogger := new(logger)
	var programLevel = new(slog.LevelVar) // Info by default

	if conf.Debug {
		programLevel.Set(slog.LevelDebug)
	}

	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})

	l := slog.New(h)

	newLogger.logger = l
	newLogger.conf = conf

	return newLogger, nil
}

func (l logger) Debug(fmtStr string, vals ...any) {
	l.logger.Debug(format(fmtStr, vals))
}

func (l logger) Info(fmtStr string, vals ...any) {
	l.logger.Info(format(fmtStr, vals))
}

func (l logger) Warning(fmtStr string, vals ...any) {
	l.logger.Warn(format(fmtStr, vals))
}

func (l logger) Error(fmtStr string, vals ...any) {
	l.logger.Error(format(fmtStr, vals))
}

func (l logger) Refresh() error {
	return nil
}

func format(fmtStr string, a []any) string {
	if len(a) == 0 {
		return fmtStr
	}

	return fmt.Sprintf(fmtStr, a...)
}
