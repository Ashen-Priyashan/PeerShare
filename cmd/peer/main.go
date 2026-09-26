package main

import (
	"encoding/json"
	"fmt"
	"io"
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

	var request protocol.Message

	err = json.NewDecoder(conn).Decode(&request)
	if err != nil {
		fmt.Println("Error decoding request:", err)
		return
	}

	fmt.Println("File requested:", request.FileName)

	file, err := os.Open("shared/" + request.FileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		fmt.Println("Error reading file information:", err)
		return
	}

	fileInfo := protocol.Message{
		Type:     protocol.FileInfo,
		PeerID:   "peer-a",
		FileName: info.Name(),
		FileSize: info.Size(),
	}

	err = json.NewEncoder(conn).Encode(fileInfo)
	if err != nil {
		fmt.Println("Error sending file information:", err)
		return
	}

	fmt.Println("Sending file:", info.Name())
	fmt.Println("File size:", info.Size(), "bytes")

	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Println("Error sending file:", err)
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

	request := protocol.Message{
		Type:     protocol.FileRequest,
		PeerID:   "peer-b",
		FileName: "test.txt",
	}

	err = json.NewEncoder(conn).Encode(request)
	if err != nil {
		fmt.Println("Error sending file request:", err)
		return
	}

	fmt.Println("File request sent!")

	var fileInfo protocol.Message

	err = json.NewDecoder(conn).Decode(&fileInfo)
	if err != nil {
		fmt.Println("Error reading file information:", err)
		return
	}

	fmt.Println("Receiving file:", fileInfo.FileName)
	fmt.Println("File size:", fileInfo.FileSize, "bytes")

	file, err := os.Create("received_" + fileInfo.FileName)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer file.Close()

	_, err = io.CopyN(file, conn, fileInfo.FileSize)
	if err != nil {
		fmt.Println("Error receiving file:", err)
		return
	}

	fmt.Println("File received successfully!")
}
