package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {

	if Debug {
		// f, err := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// fmt.Fprintf(f, format, a...)
		// f.Close()
		log.Printf(format, a...)
	}
}
