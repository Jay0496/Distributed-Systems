// Package bench contains the benchmark harness: a deterministic load generator,
// latency and throughput aggregation, storage-overhead measurement, recovery
// polling, and CSV/JSON/report writers.
//
// The package is intentionally client-side. It talks to the public
// StorageService API over injected clients so the same harness runs against
// Docker Compose and against the in-process five-node test cluster. It never
// mutates cluster state except through documented RPCs and never imports test
// code.
package bench
