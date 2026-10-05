//go:build !windows

package peer

import (
	"fmt"
	"net"
	"syscall"
)

func enableBroadcast(conn *net.UDPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("get discovery socket: %w", err)
	}
	var socketErr error
	if err := rawConn.Control(func(fd uintptr) {
		socketErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	}); err != nil {
		return fmt.Errorf("control discovery socket: %w", err)
	}
	if socketErr != nil {
		return fmt.Errorf("enable UDP broadcast: %w", socketErr)
	}
	return nil
}
