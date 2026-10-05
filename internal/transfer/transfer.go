// Package transfer implements the single- and multi-peer file transfer flows.
package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/chunk"
	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
)

const (
	maxParallelPeers = 8
	peerDialTimeout  = 5 * time.Second
	peerReadTimeout  = 2 * time.Minute
)

// Serve handles file metadata requests and any number of indexed chunk
// requests on one connection. The client ends the session with TRANSFER_DONE.
func Serve(conn io.ReadWriter, sharedDir string) error {
	request, err := protocol.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("read file request: %w", err)
	}
	if request.Type != protocol.FileRequest {
		return fmt.Errorf("expected %s, got %s", protocol.FileRequest, request.Type)
	}
	if request.FileName == "" || filepath.Base(request.FileName) != request.FileName || request.FileName == "." {
		return fmt.Errorf("invalid file name %q", request.FileName)
	}

	root, err := os.OpenRoot(sharedDir)
	if err != nil {
		return fmt.Errorf("open shared directory: %w", err)
	}
	defer root.Close()

	file, err := root.Open(request.FileName)
	if err != nil {
		return fmt.Errorf("open shared file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("read shared file information: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("shared path %q is not a regular file", request.FileName)
	}

	chunks, err := chunk.Plan(info.Size(), chunk.DefaultSize)
	if err != nil {
		return fmt.Errorf("plan file chunks: %w", err)
	}
	fileHash, err := hashRange(file, 0, info.Size())
	if err != nil {
		return fmt.Errorf("hash shared file: %w", err)
	}
	fileInfo := protocol.Message{
		Type:        protocol.FileInfo,
		PeerID:      "peer-a",
		FileName:    info.Name(),
		FileSize:    info.Size(),
		TotalChunks: len(chunks),
		ChunkSize:   chunk.DefaultSize,
		FileSHA256:  fileHash,
	}
	if err := protocol.WriteMessage(conn, fileInfo); err != nil {
		return fmt.Errorf("send file information: %w", err)
	}

	for {
		chunkRequest, err := protocol.ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("read chunk request: %w", err)
		}
		if chunkRequest.Type == protocol.TransferDone {
			return nil
		}
		if chunkRequest.Type != protocol.ChunkRequest || chunkRequest.ChunkIndex < 0 || chunkRequest.ChunkIndex >= len(chunks) {
			return fmt.Errorf("invalid chunk request type %s index %d", chunkRequest.Type, chunkRequest.ChunkIndex)
		}

		fileChunk := chunks[chunkRequest.ChunkIndex]
		chunkHash, err := hashRange(file, fileChunk.Offset, fileChunk.Size)
		if err != nil {
			return fmt.Errorf("hash chunk %d: %w", fileChunk.Index, err)
		}
		chunkInfo := protocol.Message{
			Type:        protocol.ChunkInfo,
			ChunkIndex:  fileChunk.Index,
			TotalChunks: fileChunk.Total,
			ChunkSize:   fileChunk.Size,
			ChunkSHA256: chunkHash,
		}
		if err := protocol.WriteMessage(conn, chunkInfo); err != nil {
			return fmt.Errorf("send information for chunk %d: %w", fileChunk.Index, err)
		}
		if _, err := io.CopyN(conn, io.NewSectionReader(file, fileChunk.Offset, fileChunk.Size), fileChunk.Size); err != nil {
			return fmt.Errorf("send chunk %d: %w", fileChunk.Index, err)
		}
	}
}

// Download requests all chunks from one peer and atomically writes the
// completed file to outputPath.
func Download(conn io.ReadWriter, fileName, outputPath string) error {
	fileInfo, chunks, err := requestFileInfo(conn, fileName)
	if err != nil {
		return err
	}
	tempFile, tempPath, outputDir, outputName, err := createTemporaryOutput(outputPath)
	if err != nil {
		return err
	}
	complete := false
	defer cleanupOutput(tempFile, tempPath, &complete)

	for _, fileChunk := range chunks {
		if err := downloadChunk(conn, tempFile, fileChunk, len(chunks)); err != nil {
			return err
		}
	}
	if err := protocol.WriteMessage(conn, protocol.Message{Type: protocol.TransferDone}); err != nil {
		return fmt.Errorf("finish transfer session: %w", err)
	}
	if err := verifyFile(tempFile, fileInfo.FileSHA256); err != nil {
		return err
	}
	if err := publishOutput(tempFile, tempPath, outputDir, outputName); err != nil {
		return err
	}
	complete = true
	return nil
}

// DownloadFromPeers distributes chunk requests across up to eight compatible
// peers. Peers must advertise the same whole-file digest and chunk layout.
func DownloadFromPeers(addresses []string, fileName, outputPath string) error {
	if len(addresses) == 0 {
		return errors.New("no peer addresses supplied")
	}

	sessions, fileInfo, chunks, err := openPeerSessions(addresses, fileName)
	if err != nil {
		return err
	}
	defer closeSessions(sessions)

	tempFile, tempPath, outputDir, outputName, err := createTemporaryOutput(outputPath)
	if err != nil {
		return err
	}
	complete := false
	defer cleanupOutput(tempFile, tempPath, &complete)
	if err := tempFile.Truncate(fileInfo.FileSize); err != nil {
		return fmt.Errorf("size temporary output file: %w", err)
	}

	workerCount := min(len(sessions), len(chunks), maxParallelPeers)
	if workerCount > 0 {
		var workers sync.WaitGroup
		workerErrors := make(chan error, workerCount)
		for workerIndex, session := range sessions[:workerCount] {
			workers.Add(1)
			go func(session peerSession, firstChunk int) {
				defer workers.Done()
				workerErrors <- downloadChunks(session, sessions, chunks, firstChunk, workerCount, fileInfo, tempFile)
			}(session, workerIndex)
		}
		workers.Wait()
		close(workerErrors)
		for err := range workerErrors {
			if err != nil {
				return fmt.Errorf("parallel chunk download: %w", err)
			}
		}
	}

	for _, session := range sessions[workerCount:] {
		if err := finishSession(session.conn); err != nil {
			return fmt.Errorf("finish unused peer session: %w", err)
		}
	}
	if err := verifyFile(tempFile, fileInfo.FileSHA256); err != nil {
		return err
	}
	if err := publishOutput(tempFile, tempPath, outputDir, outputName); err != nil {
		return err
	}
	complete = true
	return nil
}

type peerSession struct {
	conn    net.Conn
	address string
}

func openPeerSessions(addresses []string, fileName string) ([]peerSession, protocol.Message, []chunk.Chunk, error) {
	var sessions []peerSession
	var reference protocol.Message
	var chunks []chunk.Chunk
	var failures []error
	seen := make(map[string]struct{})

	for _, address := range addresses {
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}

		conn, err := net.DialTimeout("tcp", address, peerDialTimeout)
		if err != nil {
			failures = append(failures, fmt.Errorf("connect to %s: %w", address, err))
			continue
		}
		if err := conn.SetDeadline(time.Now().Add(peerReadTimeout)); err != nil {
			_ = conn.Close()
			failures = append(failures, fmt.Errorf("set deadline for %s: %w", address, err))
			continue
		}
		fileInfo, plan, err := requestFileInfo(conn, fileName)
		if err != nil {
			_ = conn.Close()
			failures = append(failures, fmt.Errorf("peer %s: %w", address, err))
			continue
		}
		if len(sessions) == 0 {
			reference = fileInfo
			chunks = plan
		} else if !sameFile(reference, fileInfo) {
			_ = finishSession(conn)
			_ = conn.Close()
			failures = append(failures, fmt.Errorf("peer %s advertises a different file", address))
			continue
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			_ = conn.Close()
			failures = append(failures, fmt.Errorf("clear deadline for %s: %w", address, err))
			continue
		}
		sessions = append(sessions, peerSession{conn: conn, address: address})
		if len(sessions) >= maxParallelPeers {
			break
		}
	}

	if len(sessions) == 0 {
		if len(failures) == 0 {
			return nil, protocol.Message{}, nil, errors.New("no usable peers found")
		}
		return nil, protocol.Message{}, nil, errors.Join(failures...)
	}
	return sessions, reference, chunks, nil
}

func requestFileInfo(conn io.ReadWriter, fileName string) (protocol.Message, []chunk.Chunk, error) {
	request := protocol.Message{Type: protocol.FileRequest, PeerID: "peer-b", FileName: fileName}
	if err := protocol.WriteMessage(conn, request); err != nil {
		return protocol.Message{}, nil, fmt.Errorf("send file request: %w", err)
	}
	fileInfo, err := protocol.ReadMessage(conn)
	if err != nil {
		return protocol.Message{}, nil, fmt.Errorf("read file information: %w", err)
	}
	if fileInfo.Type != protocol.FileInfo {
		return protocol.Message{}, nil, fmt.Errorf("expected %s, got %s", protocol.FileInfo, fileInfo.Type)
	}
	if fileInfo.FileName != filepath.Base(fileName) {
		return protocol.Message{}, nil, fmt.Errorf("peer returned unexpected file name %q", fileInfo.FileName)
	}
	if fileInfo.FileSize < 0 || fileInfo.ChunkSize != chunk.DefaultSize {
		return protocol.Message{}, nil, errors.New("peer returned invalid file size or unsupported chunk size")
	}
	if _, err := decodeSHA256(fileInfo.FileSHA256); err != nil {
		return protocol.Message{}, nil, fmt.Errorf("peer returned invalid file SHA-256: %w", err)
	}
	chunks, err := chunk.Plan(fileInfo.FileSize, fileInfo.ChunkSize)
	if err != nil {
		return protocol.Message{}, nil, fmt.Errorf("plan received file chunks: %w", err)
	}
	if fileInfo.TotalChunks != len(chunks) {
		return protocol.Message{}, nil, fmt.Errorf("peer reported %d chunks; file metadata requires %d", fileInfo.TotalChunks, len(chunks))
	}
	return fileInfo, chunks, nil
}

func downloadChunk(conn io.ReadWriter, output io.Writer, fileChunk chunk.Chunk, total int) error {
	if err := protocol.WriteMessage(conn, protocol.Message{Type: protocol.ChunkRequest, ChunkIndex: fileChunk.Index}); err != nil {
		return fmt.Errorf("request chunk %d: %w", fileChunk.Index, err)
	}
	chunkInfo, err := protocol.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("read information for chunk %d: %w", fileChunk.Index, err)
	}
	if err := validateChunkInfo(chunkInfo, fileChunk, total); err != nil {
		return err
	}
	hasher := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(output, hasher), conn, fileChunk.Size); err != nil {
		return fmt.Errorf("receive chunk %d: %w", fileChunk.Index, err)
	}
	if actualHash := hex.EncodeToString(hasher.Sum(nil)); actualHash != chunkInfo.ChunkSHA256 {
		return fmt.Errorf("chunk %d SHA-256 mismatch", fileChunk.Index)
	}
	return nil
}

func downloadChunks(session peerSession, sessions []peerSession, chunks []chunk.Chunk, firstChunk, step int, fileInfo protocol.Message, output *os.File) error {
	conn := session.conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	addresses := peerAddresses(sessions)
	for i := firstChunk; i < len(chunks); i += step {
		fileChunk := chunks[i]
		var err error
		if conn != nil {
			if err = conn.SetDeadline(time.Now().Add(peerReadTimeout)); err == nil {
				err = downloadChunkToFile(conn, output, fileChunk, len(chunks))
			}
			if err != nil {
				_ = conn.Close()
				conn = nil
				if retryErr := downloadChunkFromPeers(addresses, fileInfo, fileChunk, output); retryErr != nil {
					return fmt.Errorf("chunk %d failed on peer %s (%v); retries failed: %w", fileChunk.Index, session.address, err, retryErr)
				}
			}
		} else if err := downloadChunkFromPeers(addresses, fileInfo, fileChunk, output); err != nil {
			return fmt.Errorf("chunk %d retries failed: %w", fileChunk.Index, err)
		}
	}
	if conn != nil {
		if err := conn.SetDeadline(time.Now().Add(peerDialTimeout)); err != nil {
			return fmt.Errorf("set transfer completion deadline: %w", err)
		}
		return finishSession(conn)
	}
	return nil
}

func downloadChunkToFile(conn net.Conn, output *os.File, fileChunk chunk.Chunk, total int) error {
	data := new(bytes.Buffer)
	if err := downloadChunk(conn, data, fileChunk, total); err != nil {
		return err
	}
	if n, err := output.WriteAt(data.Bytes(), fileChunk.Offset); err != nil {
		return fmt.Errorf("write chunk %d: %w", fileChunk.Index, err)
	} else if n != data.Len() {
		return fmt.Errorf("write chunk %d: %w", fileChunk.Index, io.ErrShortWrite)
	}
	return nil
}

func downloadChunkFromPeers(addresses []string, expected protocol.Message, fileChunk chunk.Chunk, output *os.File) error {
	var failures []error
	for _, address := range addresses {
		conn, err := net.DialTimeout("tcp", address, peerDialTimeout)
		if err != nil {
			failures = append(failures, fmt.Errorf("connect to %s: %w", address, err))
			continue
		}
		if err := conn.SetDeadline(time.Now().Add(peerReadTimeout)); err != nil {
			_ = conn.Close()
			failures = append(failures, fmt.Errorf("set deadline for %s: %w", address, err))
			continue
		}
		fileInfo, _, err := requestFileInfo(conn, expected.FileName)
		if err == nil && !sameFile(expected, fileInfo) {
			err = errors.New("peer advertises a different file")
		}
		if err == nil {
			err = downloadChunkToFile(conn, output, fileChunk, expected.TotalChunks)
		}
		if err == nil {
			err = finishSession(conn)
		}
		_ = conn.Close()
		if err == nil {
			return nil
		}
		failures = append(failures, fmt.Errorf("peer %s: %w", address, err))
	}
	return errors.Join(failures...)
}

func peerAddresses(sessions []peerSession) []string {
	addresses := make([]string, 0, len(sessions))
	for _, session := range sessions {
		addresses = append(addresses, session.address)
	}
	return addresses
}

func validateChunkInfo(info protocol.Message, fileChunk chunk.Chunk, total int) error {
	if info.Type != protocol.ChunkInfo || info.ChunkIndex != fileChunk.Index || info.TotalChunks != total || info.ChunkSize != fileChunk.Size {
		return fmt.Errorf("peer returned invalid metadata for chunk %d", fileChunk.Index)
	}
	if _, err := decodeSHA256(info.ChunkSHA256); err != nil {
		return fmt.Errorf("peer returned invalid SHA-256 for chunk %d: %w", fileChunk.Index, err)
	}
	return nil
}

func finishSession(conn io.Writer) error {
	return protocol.WriteMessage(conn, protocol.Message{Type: protocol.TransferDone})
}

func sameFile(a, b protocol.Message) bool {
	return a.FileName == b.FileName && a.FileSize == b.FileSize && a.ChunkSize == b.ChunkSize && a.TotalChunks == b.TotalChunks && a.FileSHA256 == b.FileSHA256
}

func hashRange(source io.ReaderAt, offset, size int64) (string, error) {
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, io.NewSectionReader(source, offset, size), size); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func decodeSHA256(value string) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	if len(decoded) != sha256.Size {
		return nil, fmt.Errorf("expected %d bytes, got %d", sha256.Size, len(decoded))
	}
	return decoded, nil
}

func verifyFile(file *os.File, expectedHash string) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("read completed file information: %w", err)
	}
	actualHash, err := hashRange(file, 0, info.Size())
	if err != nil {
		return fmt.Errorf("hash completed file: %w", err)
	}
	if actualHash != expectedHash {
		return errors.New("completed file SHA-256 mismatch")
	}
	return nil
}
func createTemporaryOutput(outputPath string) (*os.File, string, string, string, error) {
	outputDir := filepath.Dir(outputPath)
	outputName := filepath.Base(outputPath)
	tempFile, err := os.CreateTemp(outputDir, ".peershare-*.partial")
	if err != nil {
		return nil, "", "", "", fmt.Errorf("create temporary output file: %w", err)
	}
	return tempFile, tempFile.Name(), outputDir, outputName, nil
}

func cleanupOutput(file *os.File, path string, complete *bool) {
	if !*complete {
		_ = file.Close()
		_ = os.Remove(path)
	}
}

func publishOutput(file *os.File, tempPath, outputDir, outputName string) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("close completed output file: %w", err)
	}
	if err := os.Rename(tempPath, filepath.Join(outputDir, outputName)); err != nil {
		return fmt.Errorf("move completed file into place: %w", err)
	}
	return nil
}

func closeSessions(sessions []peerSession) {
	for _, session := range sessions {
		_ = session.conn.Close()
	}
}
