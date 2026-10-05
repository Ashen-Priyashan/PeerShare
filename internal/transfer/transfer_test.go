package transfer

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
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
