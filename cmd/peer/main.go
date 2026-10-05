package main

import (
	"fmt"
	"net"
	"os"

	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: peer [server|client]")
		return
	}

	mode := os.Args[1]

	if mode == "server" {
		startServer()
	} else if mode == "client" {
		startClient()
	} else {
		fmt.Println("Unknown mode. Use server or client.")
	}
}

func startServer() {
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("PeerShare peer started on port 5000")
	fmt.Println("Waiting for another peer to connect...")

	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Peer connected!")

	if err := transfer.Serve(conn, "shared"); err != nil {
		fmt.Println("File transfer failed:", err)
		return
	}
	fmt.Println("File sent successfully!")
}

func startClient() {
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		fmt.Println("Error connecting to peer:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to Peer A!")

	if err := transfer.Download(conn, "test.txt", "received_test.txt"); err != nil {
		fmt.Println("File transfer failed:", err)
		return
	}

	fmt.Println("File received successfully!")
}
