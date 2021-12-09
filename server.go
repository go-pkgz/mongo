package mongo

import (
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connect to mongo url and return client. Supports expanded url params to pass a set of custom values in the url
func Connect(ctx context.Context, opts *options.ClientOptions, url string, extras ...string) (*driver.Client, map[string]interface{}, error) {
	mongoURL, extMap, err := parseExtMongoURI(url, extras)
	if err != nil {
		return nil, nil, errors.Wrap(err, "can't parse mongo url")
	}

	res, err := driver.Connect(ctx, opts.ApplyURI(mongoURL))
	if err == nil {
		err = res.Ping(ctx, nil)
	}
	return res, extMap, errors.Wrap(err, "failed to connect mongo")
}

// parseExtMongoURI extracts extra params from extras list and remove them from the url.
// Input example: mongodb://user:password@127.0.0.1:27017/test?ssl=true&ava_db=db1&ava_coll=coll1
func parseExtMongoURI(uri string, extras []string) (string, map[string]interface{}, error) {
	if uri == "" {
		return "", nil, errors.Errorf("empty url")
	}
	if len(extras) == 0 {
		return uri, nil, nil
	}
	exMap := map[string]interface{}{}

	u, err := url.Parse(uri)
	if err != nil {
		return "", nil, err
	}

	q := u.Query()
	for _, ex := range extras {
		if val := u.Query().Get(ex); val != "" {
			exMap[ex] = val
		}
		q.Del(ex)
	}
	u.RawQuery = q.Encode()
	return u.String(), exMap, nil
}

// PrepSort prepares sort params for mongo driver and returns IndexModel.
// Input string provided as [+|-]field1,[+|-]field2,[+|-]field3...
// + means ascending, - means descending. Lack of + or - in the beginning of the field name means ascending sort.
func PrepSort(sort ...string) driver.IndexModel {
	keys := bson.D{}
	for _, s := range sort {
		if s == "" {
			continue
		}
		s = strings.TrimSpace(s)
		switch s[0] {
		case '-':
			keys = append(keys, bson.E{Key: s[1:], Value: -1})
		case '+':
			keys = append(keys, bson.E{Key: s[1:], Value: 1})
		default:
			keys = append(keys, bson.E{Key: s, Value: 1})
		}
	}
	return driver.IndexModel{Keys: keys}
}

// Bind request json body from io.Reader to bson record
func Bind(r io.Reader, v interface{}) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return bson.UnmarshalExtJSON(body, false, v)
}
