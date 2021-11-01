package boltdb

import (
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

// Opens a stash instance at random temporary location.
func MustOpenRandom(ctx context.Context) *bolt.DB {
	db, err := Open(ctx, path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("golang-sdk", uuid.NewV1().String())), "data.db"))
	if err != nil {
		panic(errors.Wrap(err, "Unable to open bolt instance"))
	}
	return db
}

// Opens a stash instance at random temporary location.
func OpenRandom(ctx context.Context) (*bolt.DB, error) {
	return Open(ctx, path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("golang-sdk", uuid.NewV1().String())), "data.db"))
}

// Opens the stash instance at the given location and binds it to the context.
func Open(ctx context.Context, path string) (db *bolt.DB, err error) {
	db, err = bolt.Open(path, 0666, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return
	}

	ctx.Control().Defer(func(error) {
		afero.NewOsFs().RemoveAll(path)
	})
	ctx.Control().Defer(func(error) {
		db.Close()
	})
	return
}
