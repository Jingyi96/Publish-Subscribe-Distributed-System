package server

import (
	"fmt"
	"sync"

	"pub-sub/domain"
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

func (mgr *SubscriptionMgr) Get(topic string) []*PeerConn {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.subs[topic]
}

func (mgr *SubscriptionMgr) Subscribe(topic string, peer *PeerConn) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	fmt.Printf("[Subscribe] %s =====> Peers: %s\n", topic, peer.conn.RemoteAddr())
	if candidates, ok := mgr.subs[topic]; ok {
		for _, oldPeer := range candidates {
			if oldPeer == peer {
				return
			}
		}
		mgr.subs[topic] = append(candidates, peer)
	} else {
		mgr.subs[topic] = []*PeerConn{peer}
	}
}

func (mgr *SubscriptionMgr) Unsubscribe(topic string, peer *PeerConn) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	fmt.Printf("[Unsubscribe] %s =====> peer: %s\n", topic, peer.conn.RemoteAddr())
	if candidates, ok := mgr.subs[topic]; ok {
		newPeers := make([]*PeerConn, 0, len(candidates))
		for _, oldPeer := range candidates {
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

	mgr.flush()
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
		mgr.flush()
	}
}

func (mgr *SubscriptionMgr) flush() {
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
