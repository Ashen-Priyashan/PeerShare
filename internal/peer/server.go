// Package peer runs TCP peers and dispatches incoming connections.
package peer

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

// Serve accepts peer connections until listener is closed. Each transfer runs
// independently, and onError receives any error from an individual peer. The
// callback may be invoked concurrently.
func Serve(listener net.Listener, sharedDir string, onError func(error)) error {
	var handlers sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			handlers.Wait()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept peer connection: %w", err)
		}

		handlers.Add(1)
		go func(conn net.Conn) {
			defer handlers.Done()
			defer conn.Close()
			if err := transfer.Serve(conn, sharedDir); err != nil && onError != nil {
				onError(fmt.Errorf("peer %s: %w", conn.RemoteAddr(), err))
			}
		}(conn)
	}
}
