package bin

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

func Open(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0666, &bolt.Options{Timeout: 10 * time.Second})
}
