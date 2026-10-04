package chunk

import (
	"bytes"
	"io"
	"testing"
)

func TestPlan(t *testing.T) {
	tests := []struct {
		name      string
		fileSize  int64
		chunkSize int64
		want      []Chunk
	}{
		{name: "empty file", fileSize: 0, chunkSize: 4, want: []Chunk{}},
		{name: "single chunk", fileSize: 3, chunkSize: 4, want: []Chunk{{Index: 0, Total: 1, Offset: 0, Size: 3}}},
		{name: "exact multiple", fileSize: 8, chunkSize: 4, want: []Chunk{
			{Index: 0, Total: 2, Offset: 0, Size: 4},
			{Index: 1, Total: 2, Offset: 4, Size: 4},
		}},
		{name: "short final chunk", fileSize: 10, chunkSize: 4, want: []Chunk{
			{Index: 0, Total: 3, Offset: 0, Size: 4},
			{Index: 1, Total: 3, Offset: 4, Size: 4},
			{Index: 2, Total: 3, Offset: 8, Size: 2},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Plan(tt.fileSize, tt.chunkSize)
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if !equalChunks(got, tt.want) {
				t.Fatalf("Plan() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestPlanRejectsInvalidSizes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		fileSize  int64
		chunkSize int64
	}{
		{name: "negative file size", fileSize: -1, chunkSize: 4},
		{name: "zero chunk size", fileSize: 1, chunkSize: 0},
		{name: "negative chunk size", fileSize: 1, chunkSize: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Plan(tt.fileSize, tt.chunkSize); err == nil {
				t.Fatal("Plan() error = nil, want an error")
			}
		})
	}
}

func TestSplitAndReconstruct(t *testing.T) {
	original := []byte("PeerShare chunk reconstruction")
	var chunks []Chunk
	var data [][]byte
	err := Split(bytes.NewReader(original), int64(len(original)), 7, func(chunk Chunk, source io.Reader) error {
		part, err := io.ReadAll(source)
		if err != nil {
			return err
		}
		chunks = append(chunks, chunk)
		data = append(data, part)
		return nil
	})
	if err != nil {
		t.Fatalf("Split() error = %v", err)
	}
	wantSizes := []int{7, 7, 7, 7, 2}
	gotSizes := chunkSizes(data)
	if len(gotSizes) != len(wantSizes) {
		t.Fatalf("Split() produced chunk sizes %v, want %v", gotSizes, wantSizes)
	}
	for i := range wantSizes {
		if gotSizes[i] != wantSizes[i] {
			t.Fatalf("Split() produced chunk sizes %v, want %v", gotSizes, wantSizes)
		}
	}

	sources := make([]io.Reader, len(data))
	for i := range data {
		sources[i] = bytes.NewReader(data[i])
	}

	var got bytes.Buffer
	if err := Reconstruct(&got, chunks, sources); err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
	if !bytes.Equal(got.Bytes(), original) {
		t.Fatalf("Reconstruct() = %q, want %q", got.Bytes(), original)
	}
}

func TestSplitRejectsIncompleteChunkConsumption(t *testing.T) {
	err := Split(bytes.NewReader([]byte("abcd")), 4, 4, func(_ Chunk, source io.Reader) error {
		_, err := io.CopyN(io.Discard, source, 2)
		return err
	})
	if err == nil {
		t.Fatal("Split() error = nil, want an error when the handler leaves bytes unread")
	}
}

func TestReconstructRejectsInvalidOrderAndGaps(t *testing.T) {
	tests := []struct {
		name   string
		chunks []Chunk
		source []io.Reader
	}{
		{name: "wrong index", chunks: []Chunk{{Index: 1, Total: 1, Offset: 0, Size: 4}}, source: []io.Reader{bytes.NewReader([]byte("abcd"))}},
		{name: "gap", chunks: []Chunk{
			{Index: 0, Total: 2, Offset: 0, Size: 3},
			{Index: 1, Total: 2, Offset: 4, Size: 4},
		}, source: []io.Reader{bytes.NewReader([]byte("abc")), bytes.NewReader([]byte("defg"))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Reconstruct(&bytes.Buffer{}, tt.chunks, tt.source); err == nil {
				t.Fatal("Reconstruct() error = nil, want an error")
			}
		})
	}
}

func chunkSizes(data [][]byte) []int {
	sizes := make([]int, len(data))
	for i := range data {
		sizes[i] = len(data[i])
	}
	return sizes
}

func equalChunks(a, b []Chunk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
