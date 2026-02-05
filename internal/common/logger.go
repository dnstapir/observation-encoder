package common

type Logger interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warning(string, ...any)
	Error(string, ...any)
}
