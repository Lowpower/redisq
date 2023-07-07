// Copyright 2023 Lowpower. All rights reserved.

package redisq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Lowpower/redisq/internal/log"
)

// Server is responsible for task processing and task lifecycle management.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will schedule it for a retry.
//
// A task will be retried until either the task gets processed successfully
// or until it reaches its max retry count.
//
// If a task exhausts its retries, it will be moved to the archive and
// will be kept in the archive set.
// Note that the archive size is finite and once it reaches its max size,
// oldest tasks in the archive will be deleted.
type Server struct {
	logger *log.Logger
	state  *serverState

	// wait group to wait for all goroutines to finish.
	wg sync.WaitGroup
}

type serverStateValue int

type serverState struct {
	mu    sync.Mutex
	value serverStateValue
}

const (
	// StateNew represents a new server. Server begins in
	// this state and then transition to StatusActive when
	// Start or Run is callled.
	srvStateNew serverStateValue = iota

	// StateActive indicates the server is up and active.
	srvStateActive

	// StateStopped indicates the server is up but no longer processing new tasks.
	srvStateStopped

	// StateClosed indicates the server has been shutdown.
	srvStateClosed
)

var serverStates = []string{
	"new",
	"active",
	"stopped",
	"closed",
}

func (s serverStateValue) String() string {
	if srvStateNew <= s && s <= srvStateClosed {
		return serverStates[s]
	}
	return "unknown status"
}

// Config specifies the server's background-task processing behavior.
type Config struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the current process.
	Concurrency int

	// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
	//
	// If BaseContext is nil, the default is context.Background().
	// If this is defined, then it MUST return a non-nil context
	BaseContext func() context.Context

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	RetryDelayFunc RetryDelayFunc

	// Predicate function to determine whether the error returned from Handler is a failure.
	// If the function returns false, Server will not increment the retried counter for the task,
	// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
	// rate of the queue.
	//
	// By default, if the given error is non-nil the function returns true.
	IsFailure func(error) bool

	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	//
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	//
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	Queues map[string]int

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	StrictPriority bool

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	//
	//     func reportError(ctx context, task *redisq.Task, err error) {
	//         retried, _ := redisq.GetRetryCount(ctx)
	//         maxRetry, _ := redisq.GetMaxRetry(ctx)
	//     	   if retried >= maxRetry {
	//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	//     	   }
	//         errorReportingService.Notify(err)
	//     })
	//
	//     ErrorHandler: redisq.ErrorHandlerFunc(reportError)
	ErrorHandler ErrorHandler

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration

	// HealthCheckFunc is called periodically with any errors encountered during ping to the
	// connected redis server.
	HealthCheckFunc func(error)

	// HealthCheckInterval specifies the interval between healthchecks.
	//
	// If unset or zero, the interval is set to 15 seconds.
	HealthCheckInterval time.Duration

	// DelayedTaskCheckInterval specifies the interval between checks run on 'scheduled' and 'retry'
	// tasks, and forwarding them to 'pending' state if they are ready to be processed.
	//
	// If unset or zero, the interval is set to 5 seconds.
	DelayedTaskCheckInterval time.Duration

	// GroupGracePeriod specifies the amount of time the server will wait for an incoming task before aggregating
	// the tasks in a group. If an incoming task is received within this period, the server will wait for another
	// period of the same length, up to GroupMaxDelay if specified.
	//
	// If unset or zero, the grace period is set to 1 minute.
	// Minimum duration for GroupGracePeriod is 1 second. If value specified is less than a second, the call to
	// NewServer will panic.
	GroupGracePeriod time.Duration

	// GroupMaxDelay specifies the maximum amount of time the server will wait for incoming tasks before aggregating
	// the tasks in a group.
	//
	// If unset or zero, no delay limit is used.
	GroupMaxDelay time.Duration

	// GroupMaxSize specifies the maximum number of tasks that can be aggregated into a single task within a group.
	// If GroupMaxSize is reached, the server will aggregate the tasks into one immediately.
	//
	// If unset or zero, no size limit is used.
	GroupMaxSize int

	// GroupAggregator specifies the aggregation function used to aggregate multiple tasks in a group into one task.
	//
	// If unset or nil, the group aggregation feature will be disabled on the server.
	GroupAggregator GroupAggregator
}

// GroupAggregator aggregates a group of tasks into one before the tasks are passed to the Handler.
type GroupAggregator interface {
	// Aggregate aggregates the given tasks in a group with the given group name,
	// and returns a new task which is the aggregation of those tasks.
	//
	// Use NewTask(typename, payload, opts...) to set any options for the aggregated task.
	// The Queue option, if provided, will be ignored and the aggregated task will always be enqueued
	// to the same queue the group belonged.
	Aggregate(group string, tasks []*Task) *Task
}

// The GroupAggregatorFunc type is an adapter to allow the use of  ordinary functions as a GroupAggregator.
// If f is a function with the appropriate signature, GroupAggregatorFunc(f) is a GroupAggregator that calls f.
type GroupAggregatorFunc func(group string, tasks []*Task) *Task

// Aggregate calls fn(group, tasks)
func (fn GroupAggregatorFunc) Aggregate(group string, tasks []*Task) *Task {
	return fn(group, tasks)
}

// An ErrorHandler handles an error occurred during task processing.
type ErrorHandler interface {
	HandleError(ctx context.Context, task *Task, err error)
}

// The ErrorHandlerFunc type is an adapter to allow the use of  ordinary functions as a ErrorHandler.
// If f is a function with the appropriate signature, ErrorHandlerFunc(f) is a ErrorHandler that calls f.
type ErrorHandlerFunc func(ctx context.Context, task *Task, err error)

// HandleError calls fn(ctx, task, err)
func (fn ErrorHandlerFunc) HandleError(ctx context.Context, task *Task, err error) {
	fn(ctx, task, err)
}

// RetryDelayFunc calculates the retry delay duration for a failed task given
// the retry count, error, and the task.
//
// n is the number of times the task has been retried.
// e is the error returned by the task handler.
// t is the task in question.
type RetryDelayFunc func(n int, e error, t *Task) time.Duration

// Logger supports logging at various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})
}

// LogLevel represents logging level.
//
// It satisfies flag.Value interface.
type LogLevel int32

const (
	// Note: reserving value zero to differentiate unspecified case.
	level_unspecified LogLevel = iota

	// DebugLevel is the lowest level of logging.
	// Debug logs are intended for debugging and development purposes.
	DebugLevel

	// InfoLevel is used for general informational log messages.
	InfoLevel

	// WarnLevel is used for undesired but relatively expected events,
	// which may indicate a problem.
	WarnLevel

	// ErrorLevel is used for undesired and unexpected events that
	// the program can recover from.
	ErrorLevel

	// FatalLevel is used for undesired and unexpected events that
	// the program cannot recover from.
	FatalLevel
)

// String is part of the flag.Value interface.
func (l *LogLevel) String() string {
	switch *l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	}
	panic(fmt.Sprintf("redisq: unexpected log level: %v", *l))
}

// Set is part of the flag.Value interface.
func (l *LogLevel) Set(val string) error {
	switch strings.ToLower(val) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn", "warning":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("redisq: unsupported log level %q", val)
	}
	return nil
}
