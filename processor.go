// Copyright 2023 Lowpower. All rights reserved.

package redisq

import (
	"context"
	"sync"
	"time"

	"github.com/Lowpower/redisq/internal/base"
	"github.com/Lowpower/redisq/internal/log"
	"golang.org/x/time/rate"
)

type processor struct {
	logger *log.Logger
	broker base.Broker

	handler   Handler
	baseCtxFn func() context.Context

	queueConfig map[string]int

	// orderedQueues is set only in strict-priority mode.
	orderedQueues []string

	retryDelayFunc RetryDelayFunc
	isFailureFunc  func(error) bool

	errHandler ErrorHandler

	shutdownTimeout time.Duration

	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort chan struct{}

	starting chan<- *workerInfo
	finished chan<- *base.TaskMessage
}

type processorParams struct {
	logger          *log.Logger
	broker          base.Broker
	baseCtxFn       func() context.Context
	retryDelayFunc  RetryDelayFunc
	isFailureFunc   func(error) bool
	concurrency     int
	queues          map[string]int
	strictPriority  bool
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *workerInfo
	finished        chan<- *base.TaskMessage
}

// newProcessor constructs a new processor.
func newProcessor(params processorParams) *processor {
	return &processor{
		logger:          params.logger,
		broker:          params.broker,
		baseCtxFn:       params.baseCtxFn,
		retryDelayFunc:  params.retryDelayFunc,
		isFailureFunc:   params.isFailureFunc,
		errLogLimiter:   rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:            make(chan struct{}, params.concurrency),
		done:            make(chan struct{}),
		quit:            make(chan struct{}),
		abort:           make(chan struct{}),
		errHandler:      params.errHandler,
		shutdownTimeout: params.shutdownTimeout,
		starting:        params.starting,
		finished:        params.finished,
	}
}

// A workerInfo holds an active worker information.
type workerInfo struct {
	// the task message the worker is processing.
	msg *base.TaskMessage
	// the time the worker has started processing the message.
	started time.Time
	// deadline the worker has to finish processing the task by.
	deadline time.Time
}
