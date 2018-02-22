// KeyValue.go
package datastore

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

func NewKeyValueStore(path string, name string) (*KeyValueStore, error) {

	//make sure the path exist...
	dir, _ := filepath.Split(path)
	os.MkdirAll(dir, os.ModePerm)

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return &KeyValueStore{db, name}, nil
}

type KeyValueStore struct {
	db   *bolt.DB
	name string
}

//retrieve the bucket for the given key
func getOrCreateSubBucket(bucket *bolt.Bucket, subs []string) *bolt.Bucket {

	if len(subs) > 1 {
		for _, sub := range subs[:(len(subs) - 1)] {

			bucket_ := bucket.Bucket([]byte(sub))
			if bucket_ == nil {
				bucket, _ = bucket.CreateBucket([]byte(sub))
			} else {
				bucket = bucket_
			}
		}
	}
	return bucket
}

func (kv *KeyValueStore) Write(key string, data []byte) error {

	parts := strings.Split(key, ".")

	return kv.db.Update(func(tx *bolt.Tx) error {

		bucket := getOrCreateSubBucket(tx.Bucket([]byte(kv.name)), parts)
		return bucket.Put([]byte(parts[len(parts)-1]), data)
	})
}

func (kv *KeyValueStore) Read(key string, data []byte) ([]byte, error) {

	parts := strings.Split(key, ".")

	var result []byte
	err := kv.db.View(func(tx *bolt.Tx) error {

		bucket := getOrCreateSubBucket(tx.Bucket([]byte(kv.name)), parts)
		data := bucket.Get([]byte(parts[len(parts)-1]))
		copy(data, result)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (kv *KeyValueStore) Delete(key string) error {

	parts := strings.Split(key, ".")

	return kv.db.View(func(tx *bolt.Tx) error {

		bucket := getOrCreateSubBucket(tx.Bucket([]byte(kv.name)), parts)
		return bucket.Delete([]byte(parts[len(parts)-1]))
	})
}

func (kv *KeyValueStore) Has(key string) error {

	parts := strings.Split(key, ".")

	var result bool
	kv.db.View(func(tx *bolt.Tx) error {
		
		bucket := tx.Bucket([]byte(kv.name))
		if len(parts) > 1 {
		for _, part := range subs[:(len(parts) - 1)] {

			bucket_ := bucket.Bucket([]byte(part))
			if bucket_ == nil {
				result = false
			} else {
				bucket = bucket_
			}
		}
		result = bucket.Get([]byte(parts[len(parts)-1])) != nil
	})
	
	return result
}
