package sql

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

var (
	ErrNone     = errors.New("Sql:ErrNone")
	ErrNotEmpty = errors.New("Sql:ErrNotEmpty")
)

type PageOption func(*Page)

type Page struct {
	Offset  *uint64 `json:"offset,omitempty"`
	Limit   *uint64 `json:"limit,omitempty"`
	OrderBy *string `json:"order_by,omitempty"`
}

func buildPage(fns ...PageOption) (ret Page) {
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func LimitPtr(num *uint64) func(*Page) {
	return func(p *Page) {
		p.Limit = num
	}
}

func OffsetPtr(offset *uint64) func(*Page) {
	return func(p *Page) {
		p.Offset = offset
	}
}

func Limit(num uint64) func(*Page) {
	return func(p *Page) {
		p.Limit = &num
	}
}

func Offset(idx uint64) func(*Page) {
	return func(p *Page) {
		p.Offset = &idx
	}
}

func OrderBy(field string) func(*Page) {
	return func(p *Page) {
		p.OrderBy = &field
	}
}

func (s SelectBuilder) Page(page Page) (query SelectBuilder) {
	query = s
	if page.Offset != nil && *page.Offset > 0 {
		query = query.Offset(*page.Offset)
	}
	if page.OrderBy != nil && *page.OrderBy != "" {
		query = query.OrderBy(*page.OrderBy)
	}
	if page.Limit != nil && *page.Limit > 0 {
		query = query.Limit(*page.Limit)
	} else {
		query = query.Limit(10240)
	}

	return query
}

func IfNone(cond Query, then, els Atomic) Atomic {
	return func(tx Tx) (err error) {
		ok, err := tx.Query(Nil(), cond)
		if err != nil {
			return
		}

		if !ok {
			err = then.Exec(tx)
		} else {
			err = els.Exec(tx)
		}
		return
	}
}

func ExpectOne(query Query) Atomic {
	return func(tx Tx) (err error) {
		ok, err := tx.Query(Nil(), query)
		if !ok {
			err = errs.Or(err, errors.Wrapf(ErrNone, "Did not receive a result"))
		}
		return
	}
}

func ExpectNone(query Query) Atomic {
	return func(tx Tx) (err error) {
		ok, err := tx.Query(Nil(), query)
		if ok {
			err = errors.Wrap(ErrNotEmpty, "Expected none")
		}
		return
	}
}

func ExpectSome(query Query) Atomic {
	return func(tx Tx) (err error) {
		ok, err := tx.Query(Nil(), query)
		if !ok {
			err = errors.Wrap(ErrNone, "Expected something")
		}
		return
	}
}

func QueryOne(query Query, dest Object, found *bool) Atomic {
	return func(tx Tx) (err error) {
		*found, err = tx.Query(dest, query)
		return
	}
}

func QueryPage(query SelectBuilder, dest Buffer, page ...PageOption) Atomic {
	return func(tx Tx) (err error) {
		_, err = tx.Scan(dest, query.Page(buildPage(page...)))
		return
	}
}
