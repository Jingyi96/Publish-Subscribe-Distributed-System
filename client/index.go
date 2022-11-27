package client

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strings"

	"pub-sub/domain"
)

func Run(serverAddr string, peerAddrs []string) {
	fmt.Println("client starts")
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("error connecting to server")
		peerAddrs = append(peerAddrs[1:], peerAddrs[0])
		serverAddr = peerAddrs[0]
		Run(serverAddr, peerAddrs)
	}

	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	go func() {
		msg := domain.Message{}
		for {
			err = decoder.Decode(&msg)
			if err != nil {
				fmt.Println("error receiving message from server")
				peerAddrs = append(peerAddrs[1:], peerAddrs[0])
				serverAddr = peerAddrs[0]
				Run(serverAddr, peerAddrs)
			}
			fmt.Printf("%+v\n", msg)
		}
	}()

	initMsg := domain.HandshakeMessage{
		Type: domain.HelloFromClient,
	}
	err = encoder.Encode(initMsg)
	if err != nil {
		fmt.Println("error in sending message to server.")
		peerAddrs = append(peerAddrs[1:], peerAddrs[0])
		serverAddr = peerAddrs[0]
		Run(serverAddr, peerAddrs)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Hi! You have 3 available actions: ")
	fmt.Println("1. subscribe  : s <topic>")
	fmt.Println("2. unsubscribe: u <topic>")
	fmt.Println("3. publish    : p <topic> <message>")
	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		slices := strings.Split(strings.TrimSpace(text), " ")
		if len(slices) < 2 {
			fmt.Println("invalid command")
			continue
		}
		msg := domain.Message{
			Topic: slices[1],
		}
		switch slices[0] {
		case "s":
			msg.Type = domain.Subscribe
		case "u":
			msg.Type = domain.Unsubscribe
		case "p":
			msg.Type = domain.Normal
			if len(slices) < 3 {
				fmt.Println("invalid command")
				continue
			}
			msg.Content = strings.Join(slices[2:], " ")
		}
		err = encoder.Encode(&msg)
		if err != nil {
			fmt.Println("error sending message to server")
			peerAddrs = append(peerAddrs[1:], peerAddrs[0])
			serverAddr = peerAddrs[0]
			Run(serverAddr, peerAddrs)
		}
	}
}
