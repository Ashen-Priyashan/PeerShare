package peer

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
)

const maxConcurrentPeerProbes = 32

// PeerHealth reports whether a discovered peer accepts TCP connections.
type PeerHealth struct {
	Address   string
	Reachable bool
	Latency   time.Duration
	Error     string
}

// Scan discovers peers, then checks TCP reachability for each response.
func Scan(discoveryAddress string, discoveryTimeout, probeTimeout time.Duration) ([]PeerHealth, error) {
	if probeTimeout <= 0 {
		return nil, errors.New("probe timeout must be positive")
	}
	addresses, err := Discover(discoveryAddress, discoveryTimeout)
	if err != nil {
		return nil, err
	}
	results := make([]PeerHealth, len(addresses))
	semaphore := make(chan struct{}, maxConcurrentPeerProbes)
	var workers sync.WaitGroup
	for index, address := range addresses {
		workers.Add(1)
		go func(index int, address string) {
			defer workers.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			result := PeerHealth{Address: address}
			started := time.Now()
			conn, err := net.DialTimeout("tcp", address, probeTimeout)
			if err == nil {
				err = conn.SetDeadline(time.Now().Add(probeTimeout))
			}
			if err == nil {
				err = protocol.WriteMessage(conn, protocol.Message{Type: protocol.Ping})
			}
			if err == nil {
				var response protocol.Message
				response, err = protocol.ReadMessage(conn)
				if err == nil && response.Type != protocol.Pong {
					err = fmt.Errorf("expected %s, received %s", protocol.Pong, response.Type)
				}
			}
			result.Latency = time.Since(started)
			if err != nil {
				result.Error = err.Error()
			} else {
				result.Reachable = true
			}
			if conn != nil {
				_ = conn.Close()
			}
			results[index] = result
		}(index, address)
	}
	workers.Wait()
	return results, nil
}
