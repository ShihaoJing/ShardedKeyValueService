package raft

import (
	"log"
)

// Debug Debugging level
const Debug = 0

// DPrintf helper function to print logs
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
