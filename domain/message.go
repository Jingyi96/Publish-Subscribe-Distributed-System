package domain

import "encoding/gob"

type MessageType int

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

type Message struct {
	// the type of message
	Type    MessageType
	Id      int64
	Topic   string
	Cotent  string
	SrcAddr string
}

func Register() {
	gob.Register(Message{})
}
