package stash

import (
	"path"
	"time"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

// Opens a stash instance at random temporary location.
func OpenRandom() (*bolt.DB, error) {
	return Open(path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("stash", uuid.NewV1().String())), "stash.db"))
}

// Opens a transient stash instance that will be deleted on ctx#close().
func OpenTransient() (*bolt.DB, error) {
	stash, err := OpenRandom()
	if err != nil {
		return nil, err
	}

	return stash, nil
}

func Open(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0666, &bolt.Options{Timeout: 10 * time.Second})
}
