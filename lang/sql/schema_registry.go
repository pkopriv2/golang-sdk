package sql

// Registers and initializes the schemas with the registry.
func InitSchemas(driver Driver, reg SchemaRegistry, schemas ...Schema) error {
	for _, s := range schemas {
		err := driver.Do(func(tx Tx) error {
			return reg.Register(tx, s)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type registrySchema struct {
	Name    string `sql:"name,string,not null"`
	Version int    `sql:"version,int,not null"`
}

func newRegistrySchema(id string) Schema {
	return NewSchema(id, 1).
		WithStruct(registrySchema{}).
		WithMigration(0,
			Exec(
				AddColumn(id,
					NewColumn("version", Integer, NotNull)))).
		Build()
}

type SchemaRegistry struct {
	meta Schema
}

func NewSchemaRegistry(id string) SchemaRegistry {
	return SchemaRegistry{newRegistrySchema(id)}
}

func (r SchemaRegistry) Register(tx Tx, all ...Schema) error {
	if err := r.ensure(tx, r.meta); err != nil {
		return err
	}
	for _, cur := range all {
		if err := r.ensure(tx, cur); err != nil {
			return err
		}
	}
	return nil
}

func (r SchemaRegistry) ensure(tx Tx, next Schema) (err error) {
	if err = next.Init(tx); err != nil {
		return
	}

	current, found, err := r.readEntry(tx, next.Name)
	if err != nil {
		return
	}

	if !found {
		err = r.writeEntry(tx, next.Name, next.Version)
		return
	}

	// fmt.Fprintf(os.Stdout, "Running migrations %v: [%v-%v]\n", next.Name, current.Version, next.Version)

	deltas, err := next.Migrations(current.Version, next.Version)
	if err != nil {
		return
	}

	var n int = current.Version
	defer func() {
		if e := r.writeEntry(tx, next.Name, n); err == nil {
			err = e
		}
	}()
	var fn Atomic
	for n, fn = range deltas {
		if err = fn(tx); err != nil {
			return
		}
	}
	return
}

func (r SchemaRegistry) writeEntry(tx Tx, name string, version int) error {
	_, err := tx.Exec(
		InsertInto(r.meta.Name).
			Columns(r.meta.Cols()...).
			Values(name, version))
	return err
}

func (r SchemaRegistry) readEntry(tx Tx, name string) (s registrySchema, o bool, e error) {
	o, e = tx.Query(
		Struct(&s),
		Select(r.meta.Cols()...).
			From(r.meta.Name).
			Where("name = ?", name))
	return
}
