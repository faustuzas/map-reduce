package util

import "log"

func CloseWithErrLog(f func() error) {
	if err := f(); err != nil {
		log.Printf("[ERROR] %s", err)
	}
}
