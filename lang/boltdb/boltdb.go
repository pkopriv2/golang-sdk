package boltdb

import (
	"os"
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

// Opens a stash instance at random temporary location.
func MustOpenTemp() *bolt.DB {
	db, err := OpenTemp()
	if err != nil {
		panic(errors.Wrap(err, "Unable to open bolt instance"))
	}
	return db
}

// Opens a stash instance at random temporary location.
func OpenTemp() (*bolt.DB, error) {
	return Open(path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("golang-sdk", uuid.NewV1().String())), "data.db"))
}

// Opens the stash instance at the given location and binds it to the context.
func Open(path string) (db *bolt.DB, err error) {
	return bolt.Open(path, 0666, &bolt.Options{Timeout: 10 * time.Second})
}

// Closes the bolt instance.
func Close(db *bolt.DB) error {
	return db.Close()
}

// Deletes the bolt file
func Delete(db *bolt.DB) error {
	return os.RemoveAll(db.Path())
}

func CloseAndDelete(db *bolt.DB) error {
	return errs.Or(Close(db), Delete(db))
}
