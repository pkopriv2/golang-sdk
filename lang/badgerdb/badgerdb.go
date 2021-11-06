package badgerdb

import (
	"os"
	"path"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

// Opens a stash instance at random temporary location.
func MustOpenTemp() *badger.DB {
	db, err := OpenTemp()
	if err != nil {
		panic(errors.Wrap(err, "Unable to open badger instance"))
	}
	return db
}

// Opens a stash instance at random temporary location.
func OpenTemp() (*badger.DB, error) {
	return Open(path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("golang-sdk", uuid.NewV1().String())), "data.db"))
}

// Opens the stash instance at the given location and binds it to the context.
func Open(path string) (db *badger.DB, err error) {
	return badger.Open(badger.DefaultOptions(path).WithSyncWrites(true).WithLogger(nil))
}

// Closes the badger instance.
func Close(db *badger.DB) error {
	return db.Close()
}

// Deletes the badger file
func Delete(db *badger.DB) error {
	return os.RemoveAll(db.Opts().Dir)
}

func CloseAndDelete(db *badger.DB) error {
	return errs.Or(Close(db), Delete(db))
}
