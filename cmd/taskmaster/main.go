package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: taskmaster <command>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version":
		fmt.Println("taskmaster dev")
	default:
		fmt.Fprintf(os.Stderr, "taskmaster: unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}
