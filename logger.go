package pgqueue

// Logger is the minimal structured-logging port used by this package. It is
// deliberately a subset of common structured logger interfaces so that host
// applications can pass their existing logger without an adapter.
type Logger interface {
	Debug(msg string, keyVals ...any)
	Info(msg string, keyVals ...any)
	Warn(msg string, keyVals ...any)
	Error(msg string, keyVals ...any)
}

type nopLogger struct{}

func (nopLogger) Debug(msg string, keyVals ...any) {}
func (nopLogger) Info(msg string, keyVals ...any)  {}
func (nopLogger) Warn(msg string, keyVals ...any)  {}
func (nopLogger) Error(msg string, keyVals ...any) {}
