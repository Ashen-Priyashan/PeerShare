package transfer

import (
	"fmt"
	"os"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/chunk"
)

// TransferStats summarizes a completed download.
type TransferStats struct {
	SizeBytes             int64
	ChunkCount            int
	PeerCount             int
	Duration              time.Duration
	AverageBytesPerSecond float64
}

// DownloadFromPeersWithStats downloads a file and reports its size, chunk
// count, source count, duration, and average completed-file throughput.
func DownloadFromPeersWithStats(addresses []string, fileName, outputPath string) (TransferStats, error) {
	started := time.Now()
	if err := DownloadFromPeers(addresses, fileName, outputPath); err != nil {
		return TransferStats{Duration: time.Since(started)}, err
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		return TransferStats{Duration: time.Since(started)}, fmt.Errorf("read completed file statistics: %w", err)
	}
	chunks, err := chunk.Plan(info.Size(), chunk.DefaultSize)
	if err != nil {
		return TransferStats{Duration: time.Since(started)}, fmt.Errorf("plan completed file statistics: %w", err)
	}
	duration := time.Since(started)
	stats := TransferStats{
		SizeBytes:  info.Size(),
		ChunkCount: len(chunks),
		PeerCount:  uniquePeerCount(addresses),
		Duration:   duration,
	}
	if duration > 0 {
		stats.AverageBytesPerSecond = float64(stats.SizeBytes) / duration.Seconds()
	}
	return stats, nil
}

func uniquePeerCount(addresses []string) int {
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		seen[address] = struct{}{}
	}
	return len(seen)
}
