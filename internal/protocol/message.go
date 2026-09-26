package protocol

type MessageType string

const (
	Handshake   MessageType = "HANDSHAKE"
	TextMessage MessageType = "MESSAGE"
	FileRequest MessageType = "FILE_REQUEST"
	FileInfo    MessageType = "FILE_INFO"
)

type Message struct {
	Type     MessageType `json:"type"`
	PeerID   string      `json:"peer_id"`
	Data     string      `json:"data"`
	FileName string      `json:"file_name,omitempty"`
	FileSize int64       `json:"file_size,omitempty"`
}
