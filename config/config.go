package config

import "fmt"

var BlockSize = int64(1024)
var DBDir = "tmp"

var ErrRecordNotFound = fmt.Errorf("record not found")
var ErrInvalidParam = fmt.Errorf("invalid param")
var ErrFileNotFound = fmt.Errorf("file not found")
var ErrRecordExpired = fmt.Errorf("record expired")
var ErrNoRelatedExpire = fmt.Errorf("expire not set")
