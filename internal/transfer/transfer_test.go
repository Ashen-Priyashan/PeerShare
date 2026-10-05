package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Ashen-Priyashan/PeerShare/internal/chunk"
	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
)

func TestChunkedTransfer(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty file", data: []byte{}},
		{name: "single chunk", data: []byte("small file contents")},
		{name: "exact multiple", data: bytes.Repeat([]byte("x"), int(2*chunk.DefaultSize))},
		{name: "short final chunk", data: bytes.Repeat([]byte("p2p"), int(chunk.DefaultSize/3)+6)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sharedDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(sharedDir, "sample.bin"), tt.data, 0o600); err != nil {
				t.Fatal(err)
			}
			outputDir := t.TempDir()
			outputPath := filepath.Join(outputDir, "received.bin")

			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			defer clientConn.Close()

			serverResult := make(chan error, 1)
			go func() {
				serverResult <- Serve(serverConn, sharedDir)
			}()

			if err := Download(clientConn, "sample.bin", outputPath); err != nil {
				t.Fatalf("Download() error = %v", err)
			}
			if err := <-serverResult; err != nil {
				t.Fatalf("Serve() error = %v", err)
			}

			got, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("read downloaded file: %v", err)
			}
			if !bytes.Equal(got, tt.data) {
				t.Fatalf("downloaded data differs: got %d bytes, want %d", len(got), len(tt.data))
			}
		})
	}
}

func TestServeRejectsPathTraversal(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	serverResult := make(chan error, 1)
	sharedDir := t.TempDir()
	go func() {
		serverResult <- Serve(serverConn, sharedDir)
	}()

	request := protocol.Message{Type: protocol.FileRequest, FileName: filepath.Join("..", "outside.txt")}
	if err := protocol.WriteMessage(clientConn, request); err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err == nil {
		t.Fatal("Serve() error = nil, want an error for a path outside the shared directory")
	}
}

func TestDownloadRejectsChunkHashMismatch(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	outputDir := t.TempDir()
	outputPath := filepath.Join(outputDir, "received.bin")
	serverResult := make(chan error, 1)
	go func() {
		if _, err := protocol.ReadMessage(serverConn); err != nil {
			serverResult <- err
			return
		}
		if err := protocol.WriteMessage(serverConn, protocol.Message{
			Type:        protocol.FileInfo,
			FileName:    "sample.bin",
			FileSize:    3,
			TotalChunks: 1,
			ChunkSize:   chunk.DefaultSize,
			FileSHA256:  hashBytes([]byte("bad")),
		}); err != nil {
			serverResult <- err
			return
		}
		if _, err := protocol.ReadMessage(serverConn); err != nil {
			serverResult <- err
			return
		}
		if err := protocol.WriteMessage(serverConn, protocol.Message{
			Type:        protocol.ChunkInfo,
			TotalChunks: 1,
			ChunkSize:   3,
			ChunkSHA256: strings.Repeat("0", 64),
		}); err != nil {
			serverResult <- err
			return
		}
		_, err := io.WriteString(serverConn, "bad")
		serverResult <- err
	}()

	if err := Download(clientConn, "sample.bin", outputPath); err == nil {
		t.Fatal("Download() error = nil, want a chunk hash mismatch error")
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("fake peer error = %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output file exists after rejected chunk; stat error = %v", err)
	}
	partials, err := filepath.Glob(filepath.Join(outputDir, ".peershare-*.partial"))
	if err != nil {
		t.Fatal(err)
	}
	if len(partials) != 0 {
		t.Fatalf("partial files remain after rejected chunk: %v", partials)
	}
}

func TestDownloadRejectsWholeFileHashMismatch(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	outputPath := filepath.Join(t.TempDir(), "received.bin")
	serverResult := make(chan error, 1)
	go func() {
		if _, err := protocol.ReadMessage(serverConn); err != nil {
			serverResult <- err
			return
		}
		if err := protocol.WriteMessage(serverConn, protocol.Message{
			Type:        protocol.FileInfo,
			FileName:    "sample.bin",
			FileSize:    3,
			TotalChunks: 1,
			ChunkSize:   chunk.DefaultSize,
			FileSHA256:  hashBytes([]byte("abc")),
		}); err != nil {
			serverResult <- err
			return
		}
		if _, err := protocol.ReadMessage(serverConn); err != nil {
			serverResult <- err
			return
		}
		if err := protocol.WriteMessage(serverConn, protocol.Message{
			Type:        protocol.ChunkInfo,
			TotalChunks: 1,
			ChunkSize:   3,
			ChunkSHA256: hashBytes([]byte("bad")),
		}); err != nil {
			serverResult <- err
			return
		}
		if _, err := io.WriteString(serverConn, "bad"); err != nil {
			serverResult <- err
			return
		}
		_, err := protocol.ReadMessage(serverConn)
		serverResult <- err
	}()

	err := Download(clientConn, "sample.bin", outputPath)
	if err == nil || !strings.Contains(err.Error(), "completed file SHA-256 mismatch") {
		t.Fatalf("Download() error = %v, want completed file hash mismatch", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("fake peer error = %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output file exists after hash mismatch; stat error = %v", err)
	}
}

func TestDownloadFromMultiplePeers(t *testing.T) {
	data := bytes.Repeat([]byte("parallel PeerShare data\n"), 140_000)
	wrongData := bytes.Repeat([]byte("x"), len(data))
	addresses := []string{
		startTestPeer(t, "sample.bin", data),
		startTestPeer(t, "sample.bin", data),
		startTestPeer(t, "sample.bin", wrongData),
	}
	outputPath := filepath.Join(t.TempDir(), "received.bin")

	if err := DownloadFromPeers(addresses, "sample.bin", outputPath); err != nil {
		t.Fatalf("DownloadFromPeers() error = %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded data differs: got %d bytes, want %d", len(got), len(data))
	}
}

func startTestPeer(t *testing.T, fileName string, data []byte) string {
	t.Helper()
	sharedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedDir, fileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = Serve(conn, sharedDir)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String()
}

func hashBytes(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
