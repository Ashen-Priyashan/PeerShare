// Package transfer implements the single-peer file and chunk transfer flow.
package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Ashen-Priyashan/PeerShare/internal/chunk"
	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
)

// Serve handles one file request and streams requested chunks from sharedDir.
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
	fileInfo := protocol.Message{
		Type:        protocol.FileInfo,
		PeerID:      "peer-a",
		FileName:    info.Name(),
		FileSize:    info.Size(),
		TotalChunks: len(chunks),
		ChunkSize:   chunk.DefaultSize,
	}
	if err := protocol.WriteMessage(conn, fileInfo); err != nil {
		return fmt.Errorf("send file information: %w", err)
	}

	for i, fileChunk := range chunks {
		chunkRequest, err := protocol.ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("read request for chunk %d: %w", i, err)
		}
		if chunkRequest.Type != protocol.ChunkRequest || chunkRequest.ChunkIndex != i {
			return fmt.Errorf("expected request for chunk %d, got type %s index %d", i, chunkRequest.Type, chunkRequest.ChunkIndex)
		}

		hasher := sha256.New()
		if _, err := io.CopyN(hasher, io.NewSectionReader(file, fileChunk.Offset, fileChunk.Size), fileChunk.Size); err != nil {
			return fmt.Errorf("hash chunk %d: %w", i, err)
		}
		chunkInfo := protocol.Message{
			Type:        protocol.ChunkInfo,
			ChunkIndex:  fileChunk.Index,
			TotalChunks: fileChunk.Total,
			ChunkSize:   fileChunk.Size,
			ChunkSHA256: hex.EncodeToString(hasher.Sum(nil)),
		}
		if err := protocol.WriteMessage(conn, chunkInfo); err != nil {
			return fmt.Errorf("send information for chunk %d: %w", i, err)
		}
		if _, err := io.CopyN(conn, io.NewSectionReader(file, fileChunk.Offset, fileChunk.Size), fileChunk.Size); err != nil {
			return fmt.Errorf("send chunk %d: %w", i, err)
		}
	}
	return nil
}

// Download requests each chunk in order and atomically writes the completed
// file to outputPath. Existing output files are preserved if a transfer fails.
func Download(conn io.ReadWriter, fileName, outputPath string) error {
	request := protocol.Message{Type: protocol.FileRequest, PeerID: "peer-b", FileName: fileName}
	if err := protocol.WriteMessage(conn, request); err != nil {
		return fmt.Errorf("send file request: %w", err)
	}

	fileInfo, err := protocol.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("read file information: %w", err)
	}
	if fileInfo.Type != protocol.FileInfo {
		return fmt.Errorf("expected %s, got %s", protocol.FileInfo, fileInfo.Type)
	}
	if fileInfo.FileName != filepath.Base(fileName) {
		return fmt.Errorf("peer returned unexpected file name %q", fileInfo.FileName)
	}
	if fileInfo.FileSize < 0 || fileInfo.ChunkSize <= 0 {
		return fmt.Errorf("peer returned invalid file size or chunk size")
	}

	chunks, err := chunk.Plan(fileInfo.FileSize, fileInfo.ChunkSize)
	if err != nil {
		return fmt.Errorf("plan received file chunks: %w", err)
	}
	if fileInfo.TotalChunks != len(chunks) {
		return fmt.Errorf("peer reported %d chunks; file metadata requires %d", fileInfo.TotalChunks, len(chunks))
	}

	outputDir := filepath.Dir(outputPath)
	outputName := filepath.Base(outputPath)
	tempFile, err := os.CreateTemp(outputDir, ".peershare-*.partial")
	if err != nil {
		return fmt.Errorf("create temporary output file: %w", err)
	}
	tempPath := tempFile.Name()
	complete := false
	defer func() {
		if !complete {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()

	for _, fileChunk := range chunks {
		chunkRequest := protocol.Message{Type: protocol.ChunkRequest, ChunkIndex: fileChunk.Index}
		if err := protocol.WriteMessage(conn, chunkRequest); err != nil {
			return fmt.Errorf("request chunk %d: %w", fileChunk.Index, err)
		}

		chunkInfo, err := protocol.ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("read information for chunk %d: %w", fileChunk.Index, err)
		}
		if chunkInfo.Type != protocol.ChunkInfo || chunkInfo.ChunkIndex != fileChunk.Index || chunkInfo.TotalChunks != len(chunks) || chunkInfo.ChunkSize != fileChunk.Size {
			return fmt.Errorf("peer returned invalid metadata for chunk %d", fileChunk.Index)
		}
		hasher := sha256.New()
		if _, err := io.CopyN(io.MultiWriter(tempFile, hasher), conn, fileChunk.Size); err != nil {
			return fmt.Errorf("receive chunk %d: %w", fileChunk.Index, err)
		}
		if actualHash := hex.EncodeToString(hasher.Sum(nil)); actualHash != chunkInfo.ChunkSHA256 {
			return fmt.Errorf("chunk %d SHA-256 mismatch", fileChunk.Index)
		}
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close completed output file: %w", err)
	}
	if err := os.Rename(tempPath, filepath.Join(outputDir, outputName)); err != nil {
		return fmt.Errorf("move completed file into place: %w", err)
	}
	complete = true
	return nil
}
