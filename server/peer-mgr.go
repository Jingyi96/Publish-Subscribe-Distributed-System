package server

import (
	"fmt"
	"sync"

	"pub-sub/domain"
)

type PeerMgr struct {
	mu        sync.Mutex
	peers     map[string]*PeerConn
	ch        chan *domain.Message
	localAddr string
}

func NewPeerMgr(localAddr string) *PeerMgr {
	mgr := &PeerMgr{
		mu:        sync.Mutex{},
		peers:     make(map[string]*PeerConn),
		ch:        make(chan *domain.Message),
		localAddr: localAddr,
	}
	return mgr
}

func (mgr *PeerMgr) Put(addr string, peer *PeerConn) {
	if addr == mgr.localAddr {
		panic("local addr can not be put")
	}
	fmt.Printf("keep connection from %s (%s)\n", addr, peer.conn.RemoteAddr())
	peermutex(mgr)
	if oldConn, ok := mgr.peers[addr]; ok {
		oldConn.Close()
	}
	mgr.peers[addr] = peer

	go mgr.handlePeerMsg(addr, peer)
}

func (mgr *PeerMgr) handlePeerMsg(addr string, peer *PeerConn) {
	for !peer.IsClosed() {
		msg := &domain.Message{}
		err := peer.Read(msg)
		if err != nil {
			peer.Close()
			break
		}
		fmt.Printf("peer: %s sends %v\n", peer.conn.RemoteAddr(), msg)
		mgr.ch <- msg
	}
	mgr.ch <- &domain.Message{
		Type:    domain.ReadClosed,
		SrcAddr: addr,
	}
}

func (mgr *PeerMgr) GetChan() <-chan *domain.Message {
	return mgr.ch
}

func (mgr *PeerMgr) Get(addr string) (*PeerConn, error) {
	peermutex(mgr)
	if oldConn, ok := mgr.peers[addr]; !ok || oldConn.IsClosed() {
		peerConn, err := NewPeerFromAddr(addr, mgr.localAddr)
		if err != nil {
			delete(mgr.peers, addr)
			return nil, err
		}
		mgr.peers[addr] = peerConn
		go mgr.handlePeerMsg(addr, peerConn)
		return peerConn, nil
	} else {
		return oldConn, nil
	}
}

func (mgr *PeerMgr) Boardcast(msg *domain.Message) {
	peermutex(mgr)
	hasError := false
	for _, peer := range mgr.peers {
		if peer.IsClosed() {
			continue
		}
		err := peer.Write(msg)
		if err != nil {
			hasError = true
			peer.Close()
		}
	}
	if hasError {
		mgr.cleanup()
	}
}

func (mgr *PeerMgr) cleanup() []string {
	aboutDelete := make([]string, 0)
	active := make([]string, 0)
	for addr, peer := range mgr.peers {
		if peer.IsClosed() {
			aboutDelete = append(aboutDelete, addr)
		} else {
			active = append(active, addr)
		}
	}
	for _, addr := range aboutDelete {
		delete(mgr.peers, addr)
	}
	return active
}

func peermutex(mgr *PeerMgr) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
}
