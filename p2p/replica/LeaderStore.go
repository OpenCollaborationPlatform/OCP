package replica

import (
	"fmt"
	"sync"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

type leaderStore struct {
	adress map[uint64]Address
	keys   map[uint64]crypto.RsaPublicKey
	mutex  sync.RWMutex
}

func newLeaderStore() leaderStore {
	return leaderStore{
		adress: make(map[uint64]Address),
		keys:   make(map[uint64]crypto.RsaPublicKey),
		mutex:  sync.RWMutex{},
	}
}

func (self *leaderStore) HasEpoch(epoch uint64) bool {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	_, has := self.adress[epoch]
	return has
}

func (self *leaderStore) EpochCount() uint64 {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return uint64(len(self.adress))
}

func (self *leaderStore) GetLeaderAdressForEpoch(epoch uint64) (Address, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	addr, has := self.adress[epoch]
	if !has {
		return Address(""), fmt.Errorf("Epoch is unknown")
	}
	return addr, nil
}

func (self *leaderStore) GetLeaderKeyForEpoch(epoch uint64) (crypto.RsaPublicKey, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	key, has := self.keys[epoch]
	if !has {
		return crypto.RsaPublicKey{}, fmt.Errorf("Epoch is unknown")
	}
	return key, nil
}

func (self *leaderStore) AddEpoch(epoch uint64, addr Address, key crypto.RsaPublicKey) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.adress[epoch] = addr
	self.keys[epoch] = key
}
