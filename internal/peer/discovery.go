package peer

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

const discoveryRequest = "PEERSHARE_DISCOVER_V1"

type discoveryResponse struct {
	Magic   string `json:"magic"`
	TCPPort int    `json:"tcp_port"`
}

// ServeDiscovery answers local-network UDP discovery requests with the TCP
// port advertised by this peer. Closing conn stops the service.
func ServeDiscovery(conn *net.UDPConn, tcpPort int) error {
	if tcpPort < 1 || tcpPort > 65535 {
		return fmt.Errorf("invalid TCP port %d", tcpPort)
	}
	response, err := json.Marshal(discoveryResponse{Magic: discoveryRequest, TCPPort: tcpPort})
	if err != nil {
		return fmt.Errorf("encode discovery response: %w", err)
	}

	buffer := make([]byte, 128)
	for {
		n, address, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("read discovery request: %w", err)
		}
		if string(buffer[:n]) != discoveryRequest {
			continue
		}
		if _, err := conn.WriteToUDP(response, address); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("send discovery response: %w", err)
		}
	}
}

// Discover sends a UDP request to destination and returns responding TCP peer
// addresses. A broadcast destination finds peers on networks that permit LAN
// broadcast; a unicast destination is useful for direct discovery and testing.
func Discover(destination string, timeout time.Duration) ([]string, error) {
	if timeout <= 0 {
		return nil, errors.New("discovery timeout must be positive")
	}
	remote, err := net.ResolveUDPAddr("udp4", destination)
	if err != nil {
		return nil, fmt.Errorf("resolve discovery address: %w", err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		return nil, fmt.Errorf("open discovery socket: %w", err)
	}
	defer conn.Close()

	if err := enableBroadcast(conn); err != nil {
		return nil, err
	}
	if _, err := conn.WriteToUDP([]byte(discoveryRequest), remote); err != nil {
		return nil, fmt.Errorf("send discovery request: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set discovery timeout: %w", err)
	}

	var peers []string
	seen := make(map[string]struct{})
	buffer := make([]byte, 512)
	for {
		n, address, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
				return peers, nil
			}
			return nil, fmt.Errorf("read discovery response: %w", err)
		}

		var response discoveryResponse
		if err := json.Unmarshal(buffer[:n], &response); err != nil || response.Magic != discoveryRequest || response.TCPPort < 1 || response.TCPPort > 65535 {
			continue
		}
		peerAddress := net.JoinHostPort(address.IP.String(), strconv.Itoa(response.TCPPort))
		if _, ok := seen[peerAddress]; ok {
			continue
		}
		seen[peerAddress] = struct{}{}
		peers = append(peers, peerAddress)
	}
}
