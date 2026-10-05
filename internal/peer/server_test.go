package peer

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

func TestServeHandlesConcurrentPeers(t *testing.T) {
	const clientCount = 2
	data := bytes.Repeat([]byte("PeerShare concurrent transfer\n"), 100_000)
	sharedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedDir, "sample.bin"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverErrors := make(chan error, clientCount)
	serverResult := make(chan error, 1)
	go func() {
		serverResult <- Serve(listener, sharedDir, func(err error) {
			serverErrors <- err
		})
	}()

	connections := make([]net.Conn, clientCount)
	t.Cleanup(func() {
		_ = listener.Close()
		for _, conn := range connections {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})
	for i := range connections {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			_ = listener.Close()
			for _, openConn := range connections {
				if openConn != nil {
					_ = openConn.Close()
				}
			}
			t.Fatal(err)
		}
		connections[i] = conn
	}

	start := make(chan struct{})
	type clientResult struct {
		index int
		err   error
	}
	clientResults := make(chan clientResult, clientCount)
	outputPaths := make([]string, clientCount)
	var clients sync.WaitGroup
	for i, conn := range connections {
		outputPath := filepath.Join(t.TempDir(), "received.bin")
		outputPaths[i] = outputPath
		clients.Add(1)
		go func(index int, conn net.Conn, outputPath string) {
			defer clients.Done()
			<-start
			clientResults <- clientResult{index: index, err: transfer.Download(conn, "sample.bin", outputPath)}
		}(i, conn, outputPath)
	}
	close(start)
	clients.Wait()
	for _, conn := range connections {
		_ = conn.Close()
	}

	for i := 0; i < clientCount; i++ {
		result := <-clientResults
		if result.err != nil {
			t.Fatalf("concurrent Download() error = %v", result.err)
		}
		got, err := os.ReadFile(outputPaths[result.index])
		if err != nil {
			t.Fatalf("read concurrent download %d: %v", result.index, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("concurrent download %d differs: got %d bytes, want %d", result.index, len(got), len(data))
		}
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	select {
	case err := <-serverErrors:
		t.Fatalf("peer handler error = %v", err)
	default:
	}
}

func TestDiscoveryFindsPeer(t *testing.T) {
	address, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp4", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	serverResult := make(chan error, 1)
	go func() {
		serverResult <- ServeDiscovery(conn, 5000)
	}()

	peers, err := Discover(conn.LocalAddr().String(), time.Second)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	want := net.JoinHostPort("127.0.0.1", strconv.Itoa(5000))
	if len(peers) != 1 || peers[0] != want {
		t.Fatalf("Discover() = %v, want [%s]", peers, want)
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("ServeDiscovery() error = %v", err)
	}
}

func TestScanReportsReachablePeer(t *testing.T) {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpListener.Close()
	tcpAddress := tcpListener.Addr().(*net.TCPAddr)
	go func() {
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = transfer.Serve(conn, "")
			}(conn)
		}
	}()

	udpAddress, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	discoveryConn, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer discoveryConn.Close()
	go func() { _ = ServeDiscovery(discoveryConn, tcpAddress.Port) }()

	results, err := Scan(discoveryConn.LocalAddr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(results) != 1 || !results[0].Reachable || results[0].Error != "" {
		t.Fatalf("Scan() = %+v, want one reachable peer", results)
	}
}
