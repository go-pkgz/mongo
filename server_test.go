package mongo

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
