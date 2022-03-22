package util

import (
	"time"
)

// RetryWithMaxCount retries the function until no error was returned for max attempt count.
// The fn parameter returns two values, the boolean one means should we keep retry, the error is self-explained.
func RetryWithMaxCount(fn func() (bool, error), attempt int) error {
	for {
		shouldRetry, err := fn()
		if shouldRetry {
			return err
		}
		if err != nil {
			if attempt > 0 {
				attempt -= 1
			} else {
				return err
			}
		} else {
			return nil
		}
	}
}

func RetryInfinite(fn func() error, retryBackoff time.Duration) {
	for {
		if err := fn(); err != nil {
			time.Sleep(retryBackoff)
			continue
		}
		break
	}
}

func MustDo(fn func() error) {
	if err := fn(); err != nil {
		panic(err)
	}
}
