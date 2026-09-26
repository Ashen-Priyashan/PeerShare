package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/Ashen-Priyashan/PeerShare/internal/protocol"
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

	var message protocol.Message

	err = json.NewDecoder(conn).Decode(&message)
	if err != nil {
		fmt.Println("Error decoding message:", err)
		return
	}

	fmt.Println("Message received:")
	fmt.Println("Type:", message.Type)
	fmt.Println("Peer ID:", message.PeerID)
	fmt.Println("Data:", message.Data)

	reply := protocol.Message{
		Type:   protocol.TextMessage,
		PeerID: "peer-a",
		Data:   "Hello from Peer A!",
	}

	err = json.NewEncoder(conn).Encode(reply)
	if err != nil {
		fmt.Println("Error sending reply:", err)
		return
	}

	fmt.Println("Reply sent!")
}

func startClient() {
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		fmt.Println("Error connecting to peer:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to Peer A!")

	message := protocol.Message{
		Type:   protocol.Handshake,
		PeerID: "peer-b",
		Data:   "Hello from Peer B!",
	}

	err = json.NewEncoder(conn).Encode(message)
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	fmt.Println("Message sent!")

	var reply protocol.Message

	err = json.NewDecoder(conn).Decode(&reply)
	if err != nil {
		fmt.Println("Error reading reply:", err)
		return
	}

	fmt.Println("Reply received:")
	fmt.Println("Type:", reply.Type)
	fmt.Println("Peer ID:", reply.PeerID)
	fmt.Println("Data:", reply.Data)
}
