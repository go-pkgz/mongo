package mongo

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestServer_NewServerGood(t *testing.T) {
	mongoURL := os.Getenv("MONGO_TEST") + "/test"
	m, ext, err := Connect(context.Background(), options.Client(), mongoURL)
	defer m.Disconnect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, len(ext), "no extras")
	assert.NotNil(t, m)
}

func TestServer_NewServerBad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	_, _, err := Connect(ctx, options.Client(), "mongodb://127.0.0.1:27019/test")
	assert.NotNil(t, err)
	t.Log(err)

	_, _, err = Connect(ctx, options.Client(), "mongodb://127.0.0.1:27019/test?blah=xxx")
	assert.NotNil(t, err)
	t.Log(err)
}

func TestServer_parse(t *testing.T) {
	tbl := []struct {
		mongoURL string
		ext      []string
		cleanURL string
		extMap   map[string]interface{}
		isErr    bool
	}{
		{
			"mongodb://127.0.0.3:27017/test", nil,
			"mongodb://127.0.0.3:27017/test", nil, false,
		},
		{
			"mongodb://127.0.0.3:27017/test?blah=foo&blah2=foo2", []string{"blah", "blah2"},
			"mongodb://127.0.0.3:27017/test", map[string]interface{}{"blah": "foo", "blah2": "foo2"}, false,
		},
		{
			"mongodb://127.0.0.3:27017/test?ssl=true&blah=foo&blah2=foo2", []string{"blah", "blah2"},
			"mongodb://127.0.0.3:27017/test?ssl=true", map[string]interface{}{"blah": "foo", "blah2": "foo2"}, false,
		},
		{
			"mongodb://127.0.0.3:27017/test?blah=foo&blah2=foo2&ssl=true", []string{"blah", "blah2"},
			"mongodb://127.0.0.3:27017/test?ssl=true", map[string]interface{}{"blah": "foo", "blah2": "foo2"}, false,
		},
		{
			"", nil,
			"", nil, true,
		},
	}

	for i, tt := range tbl {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cleanURL, extMap, err := parseExtMongoURI(tt.mongoURL, tt.ext)
			if tt.isErr {
				assert.NotNil(t, err, "expect error #%d", i)
				return
			}
			require.NoError(t, err)
			assert.EqualValues(t, tt.cleanURL, cleanURL)
			assert.EqualValues(t, tt.extMap, extMap)
		})
	}
}

func TestGetMongoURLTesting(t *testing.T) {
	keepURL := os.Getenv("MONGO_TEST")
	defer os.Setenv("MONGO_TEST", keepURL)

	os.Setenv("MONGO_TEST", "mongodb://127.0.0.1:27017/test?debug=true")
	url := getMongoURL(t)
	assert.Equal(t, os.Getenv("MONGO_TEST"), url)

	os.Setenv("MONGO_TEST", "")
	url = getMongoURL(t)
	assert.Equal(t, "mongodb://mongo:27017", url)
}

func TestGetMongoURLTestingSkip(t *testing.T) {
	keepURL := os.Getenv("MONGO_TEST")
	defer os.Setenv("MONGO_TEST", keepURL)

	os.Setenv("MONGO_TEST", "skip")
	_ = getMongoURL(t)
	assert.Fail(t, "should skip")
}

func TestPrepSort(t *testing.T) {

	tbl := []struct {
		inp []string
		out bson.D
	}{
		{nil, bson.D{}},
		{[]string{"f1", " f2", "-f3 ", "+f4"}, bson.D{{"f1", 1}, {"f2", 1}, {"f3", -1}, {"f4", 1}}},
		{[]string{"+f1", " -f2", "-f3", " f4 "}, bson.D{{"f1", 1}, {"f2", -1}, {"f3", -1}, {"f4", 1}}},
	}

	for i, tt := range tbl {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			out := PrepSort(tt.inp...)
			assert.EqualValues(t, tt.out, out)
		})
	}
}

func TestPrepIndex(t *testing.T) {
	tbl := []struct {
		inp []string
		out driver.IndexModel
	}{
		{nil, driver.IndexModel{Keys: bson.D{}}},
		{[]string{"f1", " f2", "-f3 ", "+f4"}, driver.IndexModel{Keys: bson.D{{"f1", 1}, {"f2", 1}, {"f3", -1}, {"f4", 1}}}},
		{[]string{"+f1", " -f2", "-f3", " f4 "}, driver.IndexModel{Keys: bson.D{{"f1", 1}, {"f2", -1}, {"f3", -1}, {"f4", 1}}}},
	}

	for i, tt := range tbl {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			out := PrepIndex(tt.inp...)
			assert.EqualValues(t, tt.out, out)
		})
	}
}

func TestBind(t *testing.T) {
	type request struct {
		Fields     []string `json:"fields" bson:"fields"`
		Filter     bson.M   `json:"filter" bson:"filter"`
		Psrc       string   `json:"psrc" bson:"psrc"`
		SubTotals  bool     `json:"subtotals" bson:"subtotals"`
		StatFilter bson.M   `json:"stat_filter" bson:"stat_filter"`
		Sort       bson.D   `json:"sort" bson:"sort"`
		Dry        bool     `json:"dry" bson:"dry"`
		Encrypted  bool     `json:"-" bson:"-"`
	}
	body := bytes.NewBufferString(`{"fields":["cusip","acc"], "filter":{"trade_dt":{"$gte":{"$date":"2020-08-17T00:00:00-04:00"}, "$lt":{"$date":"2020-08-21T23:59:59-04:00"}}}, "psrc":"DEMO", "sort":{"day":1, "trade_id":1}, "stat_filter":{}, "subtotals":false}`)

	res := request{}
	err := Bind(body, &res)
	require.NoError(t, err)
	t.Logf("%+v", res)

	assert.Equal(t, []string{"cusip", "acc"}, res.Fields) // nolint
	assert.Equal(t, bson.M{"trade_dt": bson.M{"$gte": primitive.DateTime(1597636800000), "$lt": primitive.DateTime(1598068799000)}}, res.Filter)
	assert.Equal(t, bson.D{{"day", int32(1)}, {"trade_id", int32(1)}}, res.Sort)

	assert.Equal(t, "DEMO", res.Psrc)

	body = bytes.NewBufferString(`{"fields":["cusip","acc"], "filter":{"trade_dt":{"$gte":{"$date":"2020-08-17T00:00:00-04:00"}, "$lt":{"$date":"2020-08-21T23:59:59-04:00"}}}, "page":{"num":0, "size":50}, "psrc":"DEMO", "sort":{"trade_id":1, "day":1}, "stat_filter":{}, "subtotals":false}`)

	res = request{}
	err = Bind(body, &res)
	require.NoError(t, err)
	t.Logf("%+v", res)

	assert.Equal(t, []string{"cusip", "acc"}, res.Fields)
	assert.Equal(t, bson.M{"trade_dt": bson.M{"$gte": primitive.DateTime(1597636800000), "$lt": primitive.DateTime(1598068799000)}}, res.Filter)
	assert.Equal(t, bson.D{{"trade_id", int32(1)}, {"day", int32(1)}}, res.Sort)

	assert.Equal(t, "DEMO", res.Psrc)

	body = bytes.NewBufferString(`{"fields":["cusip","acc"], "filter":{"trade_dt":{"$gte":{"$date":"2020-08-17T04:00:00Z"}, "$lt":{"$date":"2020-08-21T23:59:59-04:00"}}}, "page":{"num":0, "size":50}, "psrc":"DEMO", "sort":{"trade_id":1, "day":1}, "stat_filter":{}, "subtotals":false}`)

	res = request{}
	err = Bind(body, &res)
	require.NoError(t, err)
	t.Logf("%+v", res)
	assert.Equal(t, bson.M{"trade_dt": bson.M{"$gte": primitive.DateTime(1597636800000), "$lt": primitive.DateTime(1598068799000)}}, res.Filter)
}
