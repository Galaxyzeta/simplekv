package util

import (
	"strings"
)

func Uint32ToBytes(a uint32) (ret []byte) {
	ret = make([]byte, 4)
	ret[0] = uint8(a >> 24)
	ret[1] = uint8(a >> 16)
	ret[2] = uint8(a >> 8)
	ret[3] = uint8(a)
	return
}

func Uint64ToBytes(a uint64) (ret []byte) {
	ret = make([]byte, 8)
	ret[0] = uint8(a >> 56)
	ret[1] = uint8(a >> 48)
	ret[2] = uint8(a >> 40)
	ret[3] = uint8(a >> 32)
	ret[4] = uint8(a >> 24)
	ret[5] = uint8(a >> 16)
	ret[6] = uint8(a >> 8)
	ret[7] = uint8(a)
	return
}

func Bytes2Uint32(k []byte) uint32 {
	_ = k[3]
	return uint32(k[3]) | uint32(k[2])<<8 | uint32(k[1])<<16 | uint32(k[0])<<24
}

func Bytes2Uint64(k []byte) uint64 {
	_ = k[7]
	return uint64(k[7]) | uint64(k[6])<<8 | uint64(k[5])<<16 | uint64(k[4])<<24 | uint64(k[3])<<32 | uint64(k[2])<<40 | uint64(k[1])<<48 | uint64(k[0])<<56
}

// StringStandardize trims string and turn it to lower case.
func StringStandardize(input string) string {
	return strings.ToLower(strings.Trim(input, " "))
}

// StringSet2List converts a string set to string list.
func StringSet2List(set map[string]struct{}) (ret []string) {
	ret = make([]string, 0, len(set))
	for str := range set {
		ret = append(ret, str)
	}
	return
}

// StringList2Set converts a string set to string list.
func StringList2Set(li []string) (set map[string]struct{}) {
	set = make(map[string]struct{}, len(li))
	for _, eachStr := range li {
		set[eachStr] = struct{}{}
	}
	return
}
