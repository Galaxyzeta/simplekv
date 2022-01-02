package util

import "time"

func FormatYYYYMMDDhhmmss(src time.Time) string {
	return src.Format("2006-01-02 15-04-05")
}
