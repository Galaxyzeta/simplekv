package util

import "strings"

func StringListContains(strlist []string, item string) bool {
	for _, iter := range strlist {
		if iter == item {
			return true
		}
	}
	return false
}

func CloneStringSet(src map[string]struct{}) (set map[string]struct{}) {
	set = make(map[string]struct{}, len(src))
	for k := range src {
		set[k] = struct{}{}
	}
	return
}

func StringSetSubtract(src map[string]struct{}, toExclude map[string]struct{}) map[string]struct{} {
	ret := map[string]struct{}{}
	for key, _ := range src {
		if _, ok := toExclude[key]; !ok {
			ret[key] = struct{}{}
		}
	}
	return ret
}

func StringBlank(str string) bool {
	str = strings.TrimSpace(str)
	return len(str) == 0
}

func StringListDelete(list []string, item string) {
	pos := -1
	for idx := range list {
		if list[idx] == item {
			pos = idx
		}
	}
	if pos != -1 {
		if pos != len(list)-1 {
			copy(list[pos:], list[pos+1:])
		} else {
			list = list[:len(list)-1]
		}
	}
}
