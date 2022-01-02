package util

import (
	"time"
)

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

func RetryInfinite(fn func() error, retryBackoff time.Duration) error {
	for {
		if err := fn(); err != nil {
			time.Sleep(retryBackoff)
			continue
		}
		break
	}
	return nil
}

func MustDo(fn func() error) {
	if err := fn(); err != nil {
		panic(err)
	}
}
