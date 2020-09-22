package raft

import (
	"log"
)

// Debug Debugging level
const Debug = 0

// DPrintf helper function to print logs
func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug >= level {
		log.Printf(format, a...)
	}
	return
}

// Max integer version of math.Max
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
