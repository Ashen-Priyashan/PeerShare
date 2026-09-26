package protocol

type MessageType string

const (
	Handshake   MessageType = "HANDSHAKE"
	TextMessage MessageType = "MESSAGE"
)

type Message struct {
	Type   MessageType `json:"type"`
	PeerID string      `json:"peer_id"`
	Data   string      `json:"data"`
}
