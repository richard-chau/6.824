package raft

import "log"

// Debugging
const Debug = 0
const DebugB = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfB(format string, a ...interface{}) (n int, err error) {
	if DebugB > 0 {
		log.Printf(format, a...)
	}
	return
}
