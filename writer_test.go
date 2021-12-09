package mongo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
)

func TestWriter(t *testing.T) {

	count := func(coll *driver.Collection) (res int) {
		count, err := coll.CountDocuments(context.Background(), bson.M{})
		assert.Nil(t, err)
		return int(count)
	}

	mg, coll, teardown := MakeTestConnection(t)
	defer teardown()

	var wr BufferedWriter = NewBufferedWriter(mg, "test", coll.Name(), 3)
	assert.Nil(t, wr.Write(bson.M{"key1": "val1"}), "write rec #1")
	assert.Nil(t, wr.Write(bson.M{"key2": "val2"}), "write rec #2")

	assert.Equal(t, 0, count(coll), "nothing yet")

	assert.Nil(t, wr.Write(bson.M{"key3": "val3"}), "write rec #3")
	assert.Equal(t, 3, count(coll), "all 3 records in")

	assert.Nil(t, wr.Write(bson.M{"key4": "val4"}), "write rec #4")
	assert.Equal(t, 3, count(coll), "still 3 records")

	assert.Nil(t, wr.Flush())
	assert.Equal(t, 4, count(coll), "all 4 records")

	assert.Nil(t, wr.Flush())
	assert.Equal(t, 4, count(coll), "still 4 records, nothing left to flush")

	assert.Nil(t, wr.Close())
}

func TestWriter_WithCollection(t *testing.T) {

	mg, coll, teardown := MakeTestConnection(t)
	defer func() {
		_ = coll.Drop(context.Background())
		teardown()
	}()
	wr := NewBufferedWriter(mg, "test", coll.Name(), 3).WithCollection("coll1")
	for i := 0; i < 100; i++ {
		require.NoError(t, wr.Write(bson.M{"key1": 1, "key2": 2}))
	}
	require.NoError(t, wr.Flush())

	coll = mg.Database("test").Collection("coll1")
	count, err := coll.CountDocuments(context.Background(), bson.M{})
	assert.Nil(t, err)
	assert.Equal(t, 100, int(count))
}

func TestWriter_Parallel(t *testing.T) {
	mg, coll, teardown := MakeTestConnection(t)
	defer teardown()

	var wg sync.WaitGroup
	wr := NewBufferedWriter(mg, "test", coll.Name(), 75)

	writeMany := func() {
		for i := 0; i < 100; i++ {
			require.NoError(t, wr.Write(bson.M{"key1": 1, "key2": 2}))
		}
		require.NoError(t, wr.Flush())
		wg.Done()
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go writeMany()
	}

	wg.Wait()

	count, err := coll.CountDocuments(context.Background(), bson.M{})
	assert.Nil(t, err)
	assert.Equal(t, 100*16, int(count))

	assert.Nil(t, wr.Close())
}

func TestWriter_WithAutoFlush(t *testing.T) {
	mg, coll, teardown := MakeTestConnection(t)
	defer teardown()

	var wr BufferedWriter = NewBufferedWriter(mg, "test", coll.Name(), 3).WithAutoFlush(300 * time.Millisecond)
	count := func() (res int) {
		count, err := coll.CountDocuments(context.Background(), bson.M{})
		assert.Nil(t, err)
		return int(count)
	}

	assert.Nil(t, wr.Write(bson.M{"key1": "val1"}), "write rec #1")
	assert.Nil(t, wr.Write(bson.M{"key2": "val2"}), "write rec #2")
	assert.Equal(t, 0, count(), "nothing yet")

	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 2, count(), "2 records flushed")

	assert.Nil(t, wr.Write(bson.M{"key3": "val3"}), "write rec #3")
	assert.Nil(t, wr.Write(bson.M{"key4": "val4"}), "write rec #4")
	assert.Nil(t, wr.Write(bson.M{"key5": "val5"}), "write rec #5")
	assert.Equal(t, 5, count(), "5 records, flushed by size, not duration")

	assert.Nil(t, wr.Write(bson.M{"key6": "val6"}), "write rec #6")
	assert.Nil(t, wr.Write(bson.M{"key7": "val7"}), "write rec #7")
	assert.Equal(t, 5, count(), "still 5 records")

	assert.Nil(t, wr.Flush())
	assert.Equal(t, 7, count(), "all 7 records")

	assert.Nil(t, wr.Flush())
	assert.Equal(t, 7, count(), "still 7 records, nothing left to flush")
	assert.Nil(t, wr.Close())
}

func TestWriter_ParallelWithAutoFlush(t *testing.T) {
	mg, coll, teardown := MakeTestConnection(t)
	defer teardown()

	var wg sync.WaitGroup
	wr := NewBufferedWriter(mg, "test", coll.Name(), 75).WithAutoFlush(time.Millisecond)

	writeMany := func() {
		for i := 0; i < 100; i++ {
			require.NoError(t, wr.Write(bson.M{"key1": 1, "key2": 2}))
			time.Sleep(time.Millisecond * 3)
		}
		wr.Flush()
		wg.Done()
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go writeMany()
	}

	wg.Wait()

	count, err := coll.CountDocuments(context.Background(), bson.M{})
	assert.Nil(t, err)
	assert.Equal(t, 100*16, int(count))

	assert.Nil(t, wr.Close())
}
