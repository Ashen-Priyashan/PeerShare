package peer

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

func TestNodeServesFilesAndDiscovery(t *testing.T) {
	sharedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedDir, "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	node, err := StartNode(sharedDir, "127.0.0.1:0", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("StartNode() error = %v", err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	peers, err := Discover(node.DiscoveryAddress(), 250*time.Millisecond)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if !reflect.DeepEqual(peers, []string{node.TCPAddress()}) {
		t.Fatalf("Discover() = %v, want [%s]", peers, node.TCPAddress())
	}

	conn, err := net.DialTimeout("tcp", node.TCPAddress(), time.Second)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	files, err := transfer.ListSharedFiles(conn)
	if err != nil {
		t.Fatalf("ListSharedFiles() error = %v", err)
	}
	want := []protocol.SharedFile{{Name: "hello.txt", Size: 5}}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("ListSharedFiles() = %#v, want %#v", files, want)
	}
}
