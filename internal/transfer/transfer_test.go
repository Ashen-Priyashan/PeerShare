package transfer

import (
	"bytes"
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
			ChunkSize:   3,
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
