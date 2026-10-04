// Package chunk splits files into ordered byte ranges and reconstructs them.
package chunk

import (
	"errors"
	"fmt"
	"io"
)

// DefaultSize is the default size of each file chunk (1 MiB).
const DefaultSize int64 = 1 << 20

// Chunk identifies a contiguous range of a file.
type Chunk struct {
	Index  int   `json:"index"`
	Total  int   `json:"total"`
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

// Plan divides a file of fileSize bytes into ordered chunks of chunkSize bytes.
// An empty file has no chunks.
func Plan(fileSize, chunkSize int64) ([]Chunk, error) {
	if fileSize < 0 {
		return nil, errors.New("file size cannot be negative")
	}
	if chunkSize <= 0 {
		return nil, errors.New("chunk size must be positive")
	}
	if fileSize == 0 {
		return []Chunk{}, nil
	}

	count := 1 + (fileSize-1)/chunkSize
	if count > int64(^uint(0)>>1) {
		return nil, errors.New("file requires too many chunks")
	}

	chunks := make([]Chunk, int(count))
	for i := range chunks {
		offset := int64(i) * chunkSize
		size := min(chunkSize, fileSize-offset)
		chunks[i] = Chunk{Index: i, Total: len(chunks), Offset: offset, Size: size}
	}
	return chunks, nil
}

// Split streams each chunk from source to handle. The handler must consume the
// chunk reader before returning; chunk bytes are not held in memory by Split.
func Split(source io.Reader, fileSize, chunkSize int64, handle func(Chunk, io.Reader) error) error {
	if handle == nil {
		return errors.New("chunk handler cannot be nil")
	}
	chunks, err := Plan(fileSize, chunkSize)
	if err != nil {
		return err
	}

	for _, chunk := range chunks {
		limited := &io.LimitedReader{R: source, N: chunk.Size}
		if err := handle(chunk, limited); err != nil {
			return fmt.Errorf("handle chunk %d: %w", chunk.Index, err)
		}
		if limited.N != 0 {
			return fmt.Errorf("handler consumed %d of %d bytes from chunk %d", chunk.Size-limited.N, chunk.Size, chunk.Index)
		}
	}
	return nil
}

// Reconstruct copies each chunk reader to dst in index order. Readers must be
// supplied in the same order as the chunk metadata.
func Reconstruct(dst io.Writer, chunks []Chunk, sources []io.Reader) error {
	if len(sources) != len(chunks) {
		return fmt.Errorf("got %d chunk readers for %d chunks", len(sources), len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.Index != i {
			return fmt.Errorf("chunk at position %d has index %d", i, chunk.Index)
		}
		if chunk.Total != len(chunks) {
			return fmt.Errorf("chunk %d reports total %d; got %d chunks", i, chunk.Total, len(chunks))
		}
		if chunk.Offset < 0 || chunk.Size <= 0 {
			return fmt.Errorf("chunk %d has invalid offset or size", i)
		}
		if i == 0 && chunk.Offset != 0 {
			return errors.New("first chunk must start at offset 0")
		}
		if i > 0 && chunk.Offset != chunks[i-1].Offset+chunks[i-1].Size {
			return fmt.Errorf("chunk %d is not contiguous with the previous chunk", i)
		}
		if _, err := io.CopyN(dst, sources[i], chunk.Size); err != nil {
			return fmt.Errorf("reconstruct chunk %d: %w", i, err)
		}
	}
	return nil
}
