package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestMessageRoundTripLeavesPayloadUnread(t *testing.T) {
	message := Message{
		Type:     FileInfo,
		PeerID:   "peer-a",
		FileName: "test.txt",
		FileSize: 4,
	}
	payload := []byte("data")

	var stream bytes.Buffer
	if err := WriteMessage(&stream, message); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}

	got, err := ReadMessage(&stream)
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if got != message {
		t.Fatalf("ReadMessage() = %#v, want %#v", got, message)
	}
	remaining, err := io.ReadAll(&stream)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, payload) {
		t.Fatalf("bytes after message = %q, want payload %q", remaining, payload)
	}
}

func TestReadMessageHandlesFragmentedReads(t *testing.T) {
	message := Message{Type: FileRequest, PeerID: "peer-b", FileName: "test.txt"}
	var stream bytes.Buffer
	if err := WriteMessage(&stream, message); err != nil {
		t.Fatal(err)
	}

	got, err := ReadMessage(oneByteReader{reader: &stream})
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if got != message {
		t.Fatalf("ReadMessage() = %#v, want %#v", got, message)
	}
}

func TestReadMessageRejectsInvalidFrames(t *testing.T) {
	var emptyFrame [4]byte
	var oversizedFrame [4]byte
	binary.BigEndian.PutUint32(oversizedFrame[:], MaxMessageSize+1)
	var truncatedFrame bytes.Buffer
	truncatedFrame.Write([]byte{0, 0, 0, 5})
	truncatedFrame.WriteString("{}")
	var malformedFrame bytes.Buffer
	malformedFrame.Write([]byte{0, 0, 0, 1, '{'})

	for _, tt := range []struct {
		name  string
		frame []byte
	}{
		{name: "zero length", frame: emptyFrame[:]},
		{name: "oversized length", frame: oversizedFrame[:]},
		{name: "truncated body", frame: truncatedFrame.Bytes()},
		{name: "invalid JSON", frame: malformedFrame.Bytes()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ReadMessage(bytes.NewReader(tt.frame)); err == nil {
				t.Fatal("ReadMessage() error = nil, want an error")
			}
		})
	}
}

func TestWriteMessageHandlesShortWrites(t *testing.T) {
	message := Message{Type: TextMessage, PeerID: "peer-a", Data: "hello"}
	var output bytes.Buffer
	if err := WriteMessage(oneByteWriter{writer: &output}, message); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	got, err := ReadMessage(&output)
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if got != message {
		t.Fatalf("ReadMessage() = %#v, want %#v", got, message)
	}
}

type oneByteReader struct {
	reader io.Reader
}

func (r oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}

type oneByteWriter struct {
	writer io.Writer
}

func (w oneByteWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return w.writer.Write(p)
}
