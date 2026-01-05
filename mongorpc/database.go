package mongorpc

// Database represents a MongoDB database.
type Database struct {
	client *Client
	name   string
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Collection returns a collection handle.
func (d *Database) Collection(name string) *Collection {
	return &Collection{
		database: d,
		name:     name,
	}
}

// ListCollections lists all collections in the database.
func (d *Database) ListCollections() ([]string, error) {
	// TODO: Implement via gRPC
	return nil, nil
}

// CreateCollection creates a new collection.
func (d *Database) CreateCollection(name string) (*Collection, error) {
	// TODO: Implement via gRPC
	return d.Collection(name), nil
}

// DropCollection drops a collection.
func (d *Database) DropCollection(name string) error {
	// TODO: Implement via gRPC
	return nil
}
