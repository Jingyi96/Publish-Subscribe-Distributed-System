package server

import (
	"encoding/gob"
	"fmt"
	"net"
	"sync"

	"pub-sub/domain"
)

type PeerConn struct {
	mu     sync.Mutex
	closed bool
	conn   net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder
}

func NewPeerFromConn(conn net.Conn) *PeerConn {
	return &PeerConn{
		mu:     sync.Mutex{},
		closed: false,
		conn:   conn,
		enc:    gob.NewEncoder(conn),
		dec:    gob.NewDecoder(conn),
	}
}

func NewPeerFromAddr(addr string, localAddr string) (*PeerConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	peerConn := NewPeerFromConn(conn)

	err = peerConn.Write(&domain.HandshakeMessage{
		Type: domain.HelloFromServerPeer,
		Addr: localAddr,
	})

	if err != nil {
		return nil, err
	}

	return peerConn, nil
}

func (p *PeerConn) Write(msg interface{}) error {
	err := p.enc.Encode(msg)
	if err != nil {
		p.Close()
	}
	return err
}

func (p *PeerConn) Read(msg interface{}) error {
	err := p.dec.Decode(msg)
	if err != nil {
		p.Close()
	}
	return err
}

func (p *PeerConn) Close() {
	if p.closed {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	fmt.Printf("close connection from %s\n", p.conn.RemoteAddr())
	p.closed = true
	p.conn.Close()
}

func (p *PeerConn) IsClosed() bool {
	return p.closed
}
