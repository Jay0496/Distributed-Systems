package main

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"distributed-kv-store/internal/bench"
)

func runReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	input := fs.String("input", "", "run directory to summarize")
	fs.Usage = func() { fmt.Fprint(stderr, "Usage: bench report --input <run-dir>\n\n"); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return flagExit(err)
	}
	if strings.TrimSpace(*input) == "" {
		fmt.Fprintln(stderr, "bench report: --input is required")
		return 2
	}
	if err := bench.RegenerateSummary(*input); err != nil {
		fmt.Fprintf(stderr, "bench report: %v\n", err)
		return 1
	}
	artifacts, err := bench.ReadRun(*input)
	if err != nil {
		fmt.Fprintf(stderr, "bench report: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, bench.RenderSummary(artifacts))
	fmt.Fprintf(stdout, "\nresults: %s\n", filepath.Clean(*input))
	return 0
}
