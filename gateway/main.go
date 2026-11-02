package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <put|get|mode> [arguments...]")
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "put":
		fmt.Println("TODO: Implement PUT logic and consistent hash routing")
	case "get":
		fmt.Println("TODO: Implement GET logic")
	case "mode":
		fmt.Println("TODO: Implement redundancy mode toggle")
	default:
		fmt.Printf("Unknown command: %s\n", command)
	}
}
