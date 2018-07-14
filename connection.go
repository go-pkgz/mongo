package mongo

import (
	"fmt"

	"github.com/globalsign/mgo"
)

// sessionFn is a function for all With*Collection calls
type sessionFn func(coll *mgo.Collection) error

// Connection allows to run request in separate session, closing automatically
type Connection struct {
	server         *Server
	db, collection string
}

// NewConnection makes a connection for server
func NewConnection(server *Server, db string, collection string) *Connection {
	return &Connection{server: server, db: db, collection: collection}
}

// WithCollection passes fun with mgo.Collection from session copy, closes it after done,
// uses Connection.DB and Connection.Collection
func (c *Connection) WithCollection(fun sessionFn) (err error) {
	return c.WithCustomCollection(c.collection, fun)
}

// WithCustomCollection passes fun with mgo.Collection from session copy, closes it after done
// uses Connection.DB or (if not defined) dial.Database, and user-defined collection
func (c *Connection) WithCustomCollection(collection string, fun sessionFn) (err error) {
	db := c.server.dial.Database
	if c.db != "" {
		db = c.db
	}
	return c.WithCustomDbCollection(db, collection, fun)
}

// WithCustomDbCollection passed fun with mgo.Collection from session copy, closes it after done
// uses passed db and collection directly.
func (c *Connection) WithCustomDbCollection(db string, collection string, fun sessionFn) (err error) {
	session := c.server.SessionCopy()
	defer session.Close()
	return fun(session.DB(db).C(collection))
}

// WithDB passes fun with mgo.Database from session copy, closes it after done
// uses Connection.DB or (if not defined) dial.Database
func (c *Connection) WithDB(fun func(dbase *mgo.Database) error) (err error) {
	db := c.server.dial.Database
	if c.db != "" {
		db = c.db
	}
	return c.WithCustomDB(db, fun)
}

// WithCustomDB passes fun with mgo.Database from session copy, closes it after done
// uses passed db directly
func (c *Connection) WithCustomDB(db string, fun func(dbase *mgo.Database) error) (err error) {
	session := c.server.SessionCopy()
	defer session.Close()
	return fun(session.DB(db))
}

func (c *Connection) String() string {
	return fmt.Sprintf("mongo:%s, db:%s, collection:%s", c.server, c.db, c.collection)
}
