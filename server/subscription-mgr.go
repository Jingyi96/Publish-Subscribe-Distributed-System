package server

import (
	"fmt"
	"pub-sub/domain"
	"sync"
)

type SubscriptionMgr struct {
	mu   sync.Mutex
	subs map[string]([]*PeerConn)
}

func NewSubscriptionMgr() *SubscriptionMgr {
	return &SubscriptionMgr{
		mu:   sync.Mutex{},
		subs: make(map[string]([]*PeerConn)),
	}
}

func (mgr *SubscriptionMgr) Subscribe(topic string, peer *PeerConn) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	fmt.Printf("subscribe topic: %s, peer: %s\n", topic, peer.conn.RemoteAddr())
	if oldPeers, ok := mgr.subs[topic]; ok {
		for _, oldPeer := range oldPeers {
			if oldPeer == peer {
				return
			}
		}
		mgr.subs[topic] = append(oldPeers, peer)
	} else {
		mgr.subs[topic] = []*PeerConn{peer}
	}
}

func (mgr *SubscriptionMgr) Get(topic string) []*PeerConn {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.subs[topic]
}

func (mgr *SubscriptionMgr) Unsubscribe(topic string, peer *PeerConn) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	fmt.Printf("unsubscribe topic: %s, peer: %s\n", topic, peer.conn.RemoteAddr())
	if oldPeers, ok := mgr.subs[topic]; ok {
		newPeers := make([]*PeerConn, 0, len(oldPeers))
		for _, oldPeer := range oldPeers {
			if oldPeer != peer {
				newPeers = append(newPeers, oldPeer)
			}
		}
		if len(newPeers) == 0 {
			delete(mgr.subs, topic)
		} else {
			mgr.subs[topic] = newPeers
		}
	}
}

func (mgr *SubscriptionMgr) Cleanup() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.cleanup()
}

func (mgr *SubscriptionMgr) cleanup() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for topic, peers := range mgr.subs {
		newPeers := make([]*PeerConn, 0, len(peers))
		for _, peer := range peers {
			if !peer.IsClosed() {
				newPeers = append(newPeers, peer)
			}
		}
		mgr.subs[topic] = newPeers
	}
}

func (mgr *SubscriptionMgr) Publish(msg *domain.Message) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	hasError := false
	if peers, ok := mgr.subs[msg.Topic]; ok {
		for _, peer := range peers {
			err := peer.Write(msg)
			if err != nil {
				hasError = true
			}
		}
	}
	if hasError {
		mgr.cleanup()
	}
}
