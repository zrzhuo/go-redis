package utils

import _type "go-redis/interface/type"

func ToCmdLine(cmd ...string) _type.CmdLine {
	cmdLine := make([][]byte, len(cmd))
	for i, s := range cmd {
		cmdLine[i] = []byte(s)
	}
	return cmdLine
}

func ToCmdLine2(name string, args ...string) _type.CmdLine {
	cmdLine := make([][]byte, len(args)+1)
	cmdLine[0] = []byte(name)
	for i, s := range args {
		cmdLine[i+1] = []byte(s)
	}
	return cmdLine
}

func ToCmdLine3(name string, args ...[]byte) _type.CmdLine {
	cmdLine := make([][]byte, len(args)+1)
	cmdLine[0] = []byte(name)
	for i, s := range args {
		cmdLine[i+1] = s
	}
	return cmdLine
}

// Equals check whether the given value is equal
func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// ConvertRange converts redis index to go slice index
// -1 => size-1
// both inclusive [0, 10] => left inclusive right exclusive [0, 9)
// out of bound to max inbound [size, size+1] => [-1, -1]
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < -size {
		return -1, -1
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1
	}
	if end < -size {
		return -1, -1
	} else if end < 0 {
		end = size + end + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if start > end {
		return -1, -1
	}
	return int(start), int(end)
}
