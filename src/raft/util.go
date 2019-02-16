package raft

import "log"

// Debugging
const Debug = 0
const DebugB = 0
const DebugC = 0
const DebugUpper = 1

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

func DPrintfC(format string, a ...interface{}) (n int, err error) {
	if DebugC > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintUpper(format string, a ...interface{}) (n int, err error) {
	if DebugUpper > 0 {
		log.Printf(format, a...)
	}
	return
}
