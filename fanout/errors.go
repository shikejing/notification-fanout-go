package fanout

import (
	"errors"
	"time"
)

var (
	errStoreClosed    = errors.New("store is closed")
	errLimiterStopped = errors.New("rate limiter stopped")
	errEngineStopped  = errors.New("engine is stopped")
	errAlreadyStarted = errors.New("engine already started")
	errUnknownChannel = errors.New("unknown channel")
	errDuplicateName  = errors.New("duplicate channel name")
)

const nsPerSecond = time.Second / time.Nanosecond
