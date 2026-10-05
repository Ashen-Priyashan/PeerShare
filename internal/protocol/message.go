package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxMessageSize limits the JSON body of a framed protocol message to 1 MiB.
const MaxMessageSize uint32 = 1 << 20

type MessageType string

const (
	Handshake    MessageType = "HANDSHAKE"
	TextMessage  MessageType = "MESSAGE"
	FileRequest  MessageType = "FILE_REQUEST"
	FileInfo     MessageType = "FILE_INFO"
	ChunkRequest MessageType = "CHUNK_REQUEST"
	ChunkInfo    MessageType = "CHUNK_INFO"
)

type Message struct {
	Type        MessageType `json:"type"`
	PeerID      string      `json:"peer_id"`
	Data        string      `json:"data"`
	FileName    string      `json:"file_name,omitempty"`
	FileSize    int64       `json:"file_size,omitempty"`
	ChunkIndex  int         `json:"chunk_index,omitempty"`
	TotalChunks int         `json:"total_chunks,omitempty"`
	ChunkSize   int64       `json:"chunk_size,omitempty"`
}

// WriteMessage writes a message as a 4-byte big-endian length followed by JSON.
func WriteMessage(w io.Writer, message Message) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode protocol message: %w", err)
	}
	if len(body) > int(MaxMessageSize) {
		return fmt.Errorf("protocol message is %d bytes; maximum is %d", len(body), MaxMessageSize)
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if err := writeFull(w, header[:]); err != nil {
		return fmt.Errorf("write protocol message length: %w", err)
	}
	if err := writeFull(w, body); err != nil {
		return fmt.Errorf("write protocol message body: %w", err)
	}
	return nil
}

// ReadMessage reads one length-prefixed JSON message without consuming bytes
// belonging to the next message or its raw file payload.
func ReadMessage(r io.Reader) (Message, error) {
	var message Message
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return message, fmt.Errorf("read protocol message length: %w", err)
	}

	bodySize := binary.BigEndian.Uint32(header[:])
	if bodySize == 0 {
		return message, errors.New("protocol message length cannot be zero")
	}
	if bodySize > MaxMessageSize {
		return message, fmt.Errorf("protocol message is %d bytes; maximum is %d", bodySize, MaxMessageSize)
	}

	body := make([]byte, bodySize)
	if _, err := io.ReadFull(r, body); err != nil {
		return message, fmt.Errorf("read protocol message body: %w", err)
	}
	if err := json.Unmarshal(body, &message); err != nil {
		return message, fmt.Errorf("decode protocol message: %w", err)
	}
	return message, nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
