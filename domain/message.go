package domain

import "encoding/gob"

type MessageType int
type HandshakeMessageType int

const (
	Normal MessageType = iota

	// client command
	Subscribe
	Unsubscribe

	// server command
	Election
	ElectionOK
	Coordinator

	// server event
	ReadClosed
)

const (
	HelloFromClient HandshakeMessageType = iota
	HelloFromServerPeer
)

type Message struct {
	// the type of message
	Type    MessageType
	Id      int64
	Topic   string
	Cotent  string
	SrcAddr string
}

type HandshakeMessage struct {
	Type HandshakeMessageType
	Addr string
}

func Register() {
	gob.Register(HandshakeMessage{})
	gob.Register(Message{})
}
