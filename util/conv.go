package util

import "strings"

func Uint32ToBytes(a uint32) (ret []byte) {
	ret = make([]byte, 4)
	ret[0] = uint8(a >> 24)
	ret[1] = uint8(a >> 16)
	ret[2] = uint8(a >> 8)
	ret[3] = uint8(a)
	return
}

func Bytes2Uint32(k []byte) uint32 {
	_ = k[3]
	return uint32(k[3]) | uint32(k[2])<<8 | uint32(k[1])<<16 | uint32(k[0])<<24
}

// StringStandardize trims string and turn it to lower case.
func StringStandardize(input string) string {
	return strings.ToLower(strings.Trim(input, " "))
}
