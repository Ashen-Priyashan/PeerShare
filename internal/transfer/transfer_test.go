package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestListSharedFiles(t *testing.T) {
	sharedDir := t.TempDir()
	for name, contents := range map[string]string{
		"z-last.txt":  "last",
		"a-first.txt": "first",
	} {
		if err := os.WriteFile(filepath.Join(sharedDir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(sharedDir, "folder"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sharedDir, "folder", "nested.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	serverResult := make(chan error, 1)
	go func() {
		serverResult <- Serve(serverConn, sharedDir)
	}()

	files, err := ListSharedFiles(clientConn)
	if err != nil {
		t.Fatalf("ListSharedFiles() error = %v", err)
	}
	want := []protocol.SharedFile{
		{Name: "a-first.txt", Size: 5},
		{Name: "z-last.txt", Size: 4},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("ListSharedFiles() = %#v, want %#v", files, want)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestListLocalFiles(t *testing.T) {
	sharedDir := t.TempDir()
	for name, contents := range map[string]string{
		"z-last.txt":  "last",
		"a-first.txt": "first",
	} {
		if err := os.WriteFile(filepath.Join(sharedDir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(sharedDir, "folder"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sharedDir, "folder", "nested.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}

	files, err := ListLocalFiles(sharedDir)
	if err != nil {
		t.Fatalf("ListLocalFiles() error = %v", err)
	}
	want := []protocol.SharedFile{
		{Name: "a-first.txt", Size: 5},
		{Name: "z-last.txt", Size: 4},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("ListLocalFiles() = %#v, want %#v", files, want)
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

func TestPeerSelectionPrefersFasterMatchingPeer(t *testing.T) {
	data := bytes.Repeat([]byte("same file"), 100)
	addresses := []string{
		startDelayedTestPeer(t, "sample.bin", data, 150*time.Millisecond),
		startTestPeer(t, "sample.bin", data),
		startTestPeer(t, "sample.bin", bytes.Repeat([]byte("other file"), 100)),
	}
	sessions, info, _, err := openPeerSessions(addresses, "sample.bin")
	if err != nil {
		t.Fatalf("openPeerSessions() error = %v", err)
	}
	defer closeSessions(sessions)
	if info.FileSHA256 != hashBytes(data) {
		t.Fatal("peer selection changed the requested file identity")
	}
	if len(sessions) != 2 || sessions[0].address != addresses[1] {
		t.Fatalf("sessions were not ranked by response time: %+v", sessions)
	}
}

func startDelayedTestPeer(t *testing.T, fileName string, data []byte, delay time.Duration) string {
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
				time.Sleep(delay)
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

func TestDownloadFromPeersWithStats(t *testing.T) {
	data := []byte("statistics test")
	address := startTestPeer(t, "sample.bin", data)
	outputPath := filepath.Join(t.TempDir(), "received.bin")

	stats, err := DownloadFromPeersWithStats([]string{address, address}, "sample.bin", outputPath)
	if err != nil {
		t.Fatalf("DownloadFromPeersWithStats() error = %v", err)
	}
	if stats.SizeBytes != int64(len(data)) || stats.ChunkCount != 1 || stats.PeerCount != 1 {
		t.Fatalf("unexpected transfer stats: %+v", stats)
	}
	if stats.Duration <= 0 || stats.AverageBytesPerSecond <= 0 {
		t.Fatalf("duration and throughput should be positive: %+v", stats)
	}
}

func TestResumeStateKeepsOnlyVerifiedChunks(t *testing.T) {
	data := bytes.Repeat([]byte("resume data"), int(chunk.DefaultSize/11)+20)
	plan, err := chunk.Plan(int64(len(data)), chunk.DefaultSize)
	if err != nil {
		t.Fatal(err)
	}
	info := protocol.Message{FileName: "sample.bin", FileSize: int64(len(data)), ChunkSize: chunk.DefaultSize, TotalChunks: len(plan), FileSHA256: hashBytes(data)}
	outputPath := filepath.Join(t.TempDir(), "received.bin")

	state, err := openResumableOutput(outputPath, info, plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.writeChunk(plan[0], data[:plan[0].Size]); err != nil {
		t.Fatal(err)
	}
	partialPath := state.file.Name()
	if err := state.close(); err != nil {
		t.Fatal(err)
	}

	state, err = openResumableOutput(outputPath, info, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !state.isComplete(0) || state.isComplete(1) {
		t.Fatal("resume state did not restore the completed chunk")
	}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(partialPath, []byte("corrupted"), 0o600); err != nil {
		t.Fatal(err)
	}
	state, err = openResumableOutput(outputPath, info, plan)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	if state.isComplete(0) {
		t.Fatal("corrupted partial chunk was trusted")
	}
}

func TestDownloadFromPeersResumesPartialFile(t *testing.T) {
	data := bytes.Repeat([]byte("resume across runs"), int(chunk.DefaultSize/18)+30)
	plan, err := chunk.Plan(int64(len(data)), chunk.DefaultSize)
	if err != nil {
		t.Fatal(err)
	}
	info := protocol.Message{FileName: "sample.bin", FileSize: int64(len(data)), ChunkSize: chunk.DefaultSize, TotalChunks: len(plan), FileSHA256: hashBytes(data)}
	outputPath := filepath.Join(t.TempDir(), "received.bin")
	state, err := openResumableOutput(outputPath, info, plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.writeChunk(plan[0], data[:plan[0].Size]); err != nil {
		t.Fatal(err)
	}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}

	if err := DownloadFromPeers([]string{startTestPeer(t, "sample.bin", data)}, "sample.bin", outputPath); err != nil {
		t.Fatalf("DownloadFromPeers() resume error = %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed download differs from source")
	}
	for _, suffix := range []string{".peershare.partial", ".peershare.state"} {
		if _, err := os.Stat(outputPath + suffix); !os.IsNotExist(err) {
			t.Fatalf("resume artifact %s remains after publication", suffix)
		}
	}
}

func TestDownloadRetriesFailedChunkFromAnotherPeer(t *testing.T) {
	data := bytes.Repeat([]byte("retry me"), int(chunk.DefaultSize/8)+20)
	addresses := []string{
		startCorruptChunkPeer(t, "sample.bin", data),
		startTestPeer(t, "sample.bin", data),
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
		t.Fatal("retried download differs from source")
	}
}

func startCorruptChunkPeer(t *testing.T, fileName string, data []byte) string {
	t.Helper()
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
				request, err := protocol.ReadMessage(conn)
				if err != nil || request.Type != protocol.FileRequest {
					return
				}
				plan, err := chunk.Plan(int64(len(data)), chunk.DefaultSize)
				if err != nil {
					return
				}
				info := protocol.Message{Type: protocol.FileInfo, FileName: fileName, FileSize: int64(len(data)), TotalChunks: len(plan), ChunkSize: chunk.DefaultSize, FileSHA256: hashBytes(data)}
				if protocol.WriteMessage(conn, info) != nil {
					return
				}
				request, err = protocol.ReadMessage(conn)
				if err != nil || request.Type != protocol.ChunkRequest || request.ChunkIndex < 0 || request.ChunkIndex >= len(plan) {
					return
				}
				fileChunk := plan[request.ChunkIndex]
				chunkInfo := protocol.Message{Type: protocol.ChunkInfo, ChunkIndex: fileChunk.Index, TotalChunks: len(plan), ChunkSize: fileChunk.Size, ChunkSHA256: strings.Repeat("0", 64)}
				if protocol.WriteMessage(conn, chunkInfo) != nil {
					return
				}
				_, _ = conn.Write(bytes.Repeat([]byte("x"), int(fileChunk.Size)))
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String()
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
