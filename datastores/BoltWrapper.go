/* A small wrapper for boltDB to mimic update/view notation while having a single
transaction
*/
package datastore

import (
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

type boltWrapper struct {
	db *bolt.DB
	tx *bolt.Tx
	mutex sync.Mutex
}

func (self *boltWrapper) Begin() error {

	self.mutex.Lock()

	if self.tx != nil {
		return fmt.Errorf("Transaction already open")
	}

	tx, err := self.db.Begin(true)
	self.tx = tx
	return err
}

func (self *boltWrapper) Commit() error {

	if self.tx == nil {
		return fmt.Errorf("No transaction open to commit")
	}

	err := self.tx.Commit()
	self.tx = nil
	
	self.mutex.Unlock()
	return err
}

func (self *boltWrapper) Rollback() error {

	if self.tx == nil {
		return fmt.Errorf("No transaction open to rollback")
	}

	err := self.tx.Rollback()
	self.tx = nil

	self.mutex.Unlock()
	return err
}

func (self *boltWrapper) RollbackKeepOpen() error {

	if self.tx == nil {
		return fmt.Errorf("No transaction open to rollback")
	}

	err := self.tx.Rollback()
	if err != nil {
		return err
	}
	
	tx, err := self.db.Begin(true)
	self.tx = tx
	
	return err
}

func (self *boltWrapper) CanAccess() bool {

	return self.tx != nil
}

func (self *boltWrapper) Update(fn func(*bolt.Tx) error) error {

	if self.tx == nil {
		return fmt.Errorf("No transaction open, cannot update")
	}
	return fn(self.tx)
}

func (self *boltWrapper) View(fn func(*bolt.Tx) error) error {

	if self.tx == nil {
		return fmt.Errorf("No transaction open, cannot update")
	}
	return fn(self.tx)
}
