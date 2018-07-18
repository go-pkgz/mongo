package mongo

import (
	"fmt"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRecord struct {
	Symbol string
	Num    int
}

func TestConnection_WithCollection(t *testing.T) {
	c, err := write(t)
	require.NoError(t, err)
	defer RemoveTestCollection(t, c)

	var res []testRecord
	err = c.WithCollection(func(coll *mgo.Collection) error {
		return coll.Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))

	err = c.WithCollection(func(coll *mgo.Collection) error {
		return coll.Find(bson.M{"symbol": "blah"}).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))

	r1 := testRecord{}
	err = c.WithCollection(func(coll *mgo.Collection) error {
		return coll.Find(bson.M{"symbol": "blah"}).One(&r1)
	})
	assert.Equal(t, mgo.ErrNotFound, err)

	c = NewConnection(c.server, "test", "bbbbbbbaaad")
	err = c.WithCollection(func(coll *mgo.Collection) error {
		return coll.Find(bson.M{"symbol": "blah"}).One(&r1)
	})
	assert.Equal(t, mgo.ErrNotFound, err)
}

func TestConnection_WithCustomCollection(t *testing.T) {
	c, err := MakeTestConnection(t)
	require.NoError(t, err)
	defer RemoveTestCollections(t, c, "coll2", "coll3")

	// write co coll2
	_ = c.WithCustomCollection("coll2", func(coll *mgo.Collection) error {
		for i := 0; i < 22; i++ {
			r := testRecord{
				Symbol: fmt.Sprintf("symb-%02d", i%5),
				Num:    i,
			}
			require.NoError(t, coll.Insert(r))
		}
		return nil
	})

	// write co coll3
	_ = c.WithCustomCollection("coll3", func(coll *mgo.Collection) error {
		for i := 0; i < 33; i++ {
			r := testRecord{
				Symbol: fmt.Sprintf("symb-%02d", i%5),
				Num:    i,
			}
			require.NoError(t, coll.Insert(r))
		}
		return nil
	})

	var res []testRecord
	err = c.WithCustomCollection("coll2", func(coll *mgo.Collection) error {
		return coll.Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 22, len(res))

	err = c.WithCustomCollection("coll3", func(coll *mgo.Collection) error {
		return coll.Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 33, len(res))
}

func TestConnection_WithCollectionNoDB(t *testing.T) {
	c, err := write(t)
	require.NoError(t, err)
	defer RemoveTestCollection(t, c)

	var res []testRecord
	err = c.WithCollection(func(coll *mgo.Collection) error {
		return coll.Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))
}

func TestConnection_WithDB(t *testing.T) {
	c, err := write(t)
	require.NoError(t, err)
	defer RemoveTestCollection(t, c)

	var res []testRecord
	err = c.WithDB(func(dbase *mgo.Database) error {
		return dbase.C(c.collection).Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))

	err = c.WithDB(func(dbase *mgo.Database) error {
		return dbase.C(c.collection).Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))
}

func TestConnection_WithCustomDB(t *testing.T) {
	c, err := MakeTestConnection(t)
	require.NoError(t, err)

	defer func() {
		err = c.WithCustomDB("test_custom", func(dbase *mgo.Database) error {
			return dbase.DropDatabase()
		})
	}()

	var res []testRecord

	c.WithCustomDbCollection("test_custom", "coll1", func(coll *mgo.Collection) error {
		for i := 0; i < 100; i++ {
			r := testRecord{
				Symbol: fmt.Sprintf("symb-%02d", i%5),
				Num:    i,
			}
			require.NoError(t, coll.Insert(r))
		}
		return nil
	})

	err = c.WithCustomDB("test_custom", func(dbase *mgo.Database) error {
		return dbase.C("coll1").Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))
}

func TestCleanup(t *testing.T) {
	c, err := write(t)
	require.NoError(t, err)

	var res []testRecord
	err = c.WithCustomDB("test", func(dbase *mgo.Database) error {
		return dbase.C(c.collection).Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, len(res))

	RemoveTestCollections(t, c, c.collection)
	err = c.WithCustomDB("test", func(dbase *mgo.Database) error {
		return dbase.C(c.collection).Find(nil).All(&res)
	})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
}

func write(t *testing.T) (*Connection, error) {
	c, err := MakeTestConnection(t)
	require.NoError(t, err)

	err = c.WithCollection(func(coll *mgo.Collection) error {
		for i := 0; i < 100; i++ {
			r := testRecord{
				Symbol: fmt.Sprintf("symb-%02d", i%5),
				Num:    i,
			}
			insertErr := coll.Insert(r)
			require.Nil(t, insertErr, fmt.Sprintf("insert %+v", r))
		}
		return nil
	})
	return c, err
}
