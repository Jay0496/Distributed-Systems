package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// Stats reports file sizes and row counts for this node's database. Call
// Checkpoint first when a measurement should exclude the write-ahead log.
func (s *SQLiteStore) Stats() (StoreStats, error) {
	if err := s.ready(); err != nil {
		return StoreStats{}, err
	}

	var stats StoreStats
	ctx := context.Background()

	queries := []struct {
		query string
		dest  *int64
	}{
		{`SELECT COUNT(*) FROM keys`, &stats.KeyCount},
		{`SELECT COUNT(*) FROM stripes`, &stats.StripeCount},
		{`SELECT COUNT(*) FROM fragments`, &stats.FragmentCount},
		{`SELECT COALESCE(SUM(length(data)), 0) FROM fragments`, &stats.FragmentBytes},
	}
	for _, item := range queries {
		if err := s.db.QueryRowContext(ctx, item.query).Scan(item.dest); err != nil {
			return StoreStats{}, fmt.Errorf("query store stats: %w", err)
		}
	}

	for _, path := range []string{s.path, s.path + "-wal", s.path + "-shm"} {
		size, err := fileSize(path)
		if err != nil {
			return StoreStats{}, err
		}
		stats.Bytes += size
	}

	return stats, nil
}

func fileSize(path string) (int64, error) {
	if path == "" {
		return 0, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("stat %s: %w", path, err)
	}
	return info.Size(), nil
}
