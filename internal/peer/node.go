package peer

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
)

// Node runs a TCP file server and a UDP discovery responder for one shared
// directory. Closing the node stops both listeners.
type Node struct {
	listener      net.Listener
	discoveryConn *net.UDPConn
	wait          sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
}

// StartNode starts the TCP and UDP services for sharedDir. A listen address
// may use port 0 to request an available port.
func StartNode(sharedDir, tcpAddress, discoveryAddress string, onError func(error)) (*Node, error) {
	info, err := os.Stat(sharedDir)
	if err != nil {
		return nil, fmt.Errorf("inspect shared directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("shared path %q is not a directory", sharedDir)
	}

	listener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		return nil, fmt.Errorf("start TCP server: %w", err)
	}
	udpAddress, err := net.ResolveUDPAddr("udp4", discoveryAddress)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("resolve discovery address: %w", err)
	}
	discoveryConn, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("start peer discovery: %w", err)
	}
	tcpListenerAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		_ = listener.Close()
		_ = discoveryConn.Close()
		return nil, errors.New("read TCP listener address")
	}

	node := &Node{listener: listener, discoveryConn: discoveryConn}
	node.wait.Add(2)
	go func() {
		defer node.wait.Done()
		if err := Serve(listener, sharedDir, onError); err != nil && onError != nil {
			onError(fmt.Errorf("peer server stopped: %w", err))
		}
	}()
	go func() {
		defer node.wait.Done()
		if err := ServeDiscovery(discoveryConn, tcpListenerAddress.Port); err != nil && onError != nil {
			onError(fmt.Errorf("peer discovery stopped: %w", err))
		}
	}()
	return node, nil
}

// TCPAddress returns the address used by this node's TCP server.
func (n *Node) TCPAddress() string {
	return n.listener.Addr().String()
}

// DiscoveryAddress returns the address used by this node's UDP discovery responder.
func (n *Node) DiscoveryAddress() string {
	return n.discoveryConn.LocalAddr().String()
}

// Close stops the TCP and UDP listeners and waits for their serve loops.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		var closeErrors []error
		if err := n.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, fmt.Errorf("close TCP server: %w", err))
		}
		if err := n.discoveryConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, fmt.Errorf("close peer discovery: %w", err))
		}
		n.wait.Wait()
		n.closeErr = errors.Join(closeErrors...)
	})
	return n.closeErr
}
