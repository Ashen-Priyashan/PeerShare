package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Ashen-Priyashan/PeerShare/internal/chunk"
	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
)

const resumeStateVersion = 1

type resumeState struct {
	mu        sync.Mutex
	file      *os.File
	statePath string
	fileInfo  protocol.Message
	chunks    []chunk.Chunk
	completed []string
}

type resumeManifest struct {
	Version   int      `json:"version"`
	FileName  string   `json:"file_name"`
	FileSize  int64    `json:"file_size"`
	ChunkSize int64    `json:"chunk_size"`
	FileSHA   string   `json:"file_sha256"`
	Completed []string `json:"completed_chunks"`
}

func openResumableOutput(outputPath string, info protocol.Message, chunks []chunk.Chunk) (*resumeState, error) {
	partialPath := outputPath + ".peershare.partial"
	statePath := outputPath + ".peershare.state"
	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open partial download: %w", err)
	}
	state := &resumeState{file: file, statePath: statePath, fileInfo: info, chunks: chunks, completed: make([]string, len(chunks))}
	if err := state.load(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return state, nil
}

func (s *resumeState) load() error {
	manifestData, err := os.ReadFile(s.statePath)
	if err == nil {
		var manifest resumeManifest
		if json.Unmarshal(manifestData, &manifest) == nil && s.matches(manifest) {
			for index, expectedHash := range manifest.Completed {
				if expectedHash == "" || index >= len(s.chunks) {
					continue
				}
				actualHash, err := hashRange(s.file, s.chunks[index].Offset, s.chunks[index].Size)
				if err == nil && actualHash == expectedHash {
					s.completed[index] = expectedHash
				}
			}
			return s.file.Truncate(s.fileInfo.FileSize)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read resume state: %w", err)
	}
	if err := s.file.Truncate(s.fileInfo.FileSize); err != nil {
		return fmt.Errorf("reset partial download: %w", err)
	}
	return s.saveLocked()
}

func (s *resumeState) matches(manifest resumeManifest) bool {
	return manifest.Version == resumeStateVersion && manifest.FileName == s.fileInfo.FileName && manifest.FileSize == s.fileInfo.FileSize && manifest.ChunkSize == s.fileInfo.ChunkSize && manifest.FileSHA == s.fileInfo.FileSHA256 && len(manifest.Completed) == len(s.chunks)
}

func (s *resumeState) isComplete(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed[index] != ""
}

func (s *resumeState) writeChunk(fileChunk chunk.Chunk, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed[fileChunk.Index] != "" {
		return nil
	}
	if int64(len(data)) != fileChunk.Size {
		return fmt.Errorf("chunk %d has unexpected size", fileChunk.Index)
	}
	if n, err := s.file.WriteAt(data, fileChunk.Offset); err != nil {
		return fmt.Errorf("write chunk %d to partial file: %w", fileChunk.Index, err)
	} else if n != len(data) {
		return fmt.Errorf("write chunk %d to partial file: %w", fileChunk.Index, io.ErrShortWrite)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("save chunk %d to partial file: %w", fileChunk.Index, err)
	}
	hash := sha256.Sum256(data)
	s.completed[fileChunk.Index] = hex.EncodeToString(hash[:])
	if err := s.saveLocked(); err != nil {
		s.completed[fileChunk.Index] = ""
		return err
	}
	return nil
}

func (s *resumeState) saveLocked() error {
	manifest := resumeManifest{Version: resumeStateVersion, FileName: s.fileInfo.FileName, FileSize: s.fileInfo.FileSize, ChunkSize: s.fileInfo.ChunkSize, FileSHA: s.fileInfo.FileSHA256, Completed: s.completed}
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode resume state: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.statePath), ".peershare-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create resume state: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write resume state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("save resume state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close resume state: %w", err)
	}
	if err := os.Rename(tempPath, s.statePath); err != nil {
		return fmt.Errorf("publish resume state: %w", err)
	}
	return nil
}

func (s *resumeState) close() error {
	return s.file.Close()
}

func (s *resumeState) remove() {
	_ = os.Remove(s.file.Name())
	_ = os.Remove(s.statePath)
}
