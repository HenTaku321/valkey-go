package valkey

import (
	"context"
	"crypto/tls"
	"github.com/valkey-io/valkey-go"
	"time"
)

type Client struct {
	C   valkey.Client
	Ctx context.Context
}

// NewClient 若启用 tls 但无更多配置需传入 &tls.Config{} 而不是 nil
func NewClient(address, password string, db int, forceSingleClient bool, tlsConfig *tls.Config) (*Client, error) {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}, Password: password, SelectDB: db, ForceSingleClient: forceSingleClient, TLSConfig: tlsConfig})
	if err != nil {
		return nil, err
	}

	return &Client{client, context.Background()}, nil
}

func (c *Client) Delete(key string) error {
	return c.C.Do(c.Ctx, c.C.B().Del().Key(key).Build()).Error()
}

func (c *Client) SetString(key, value string, ex *time.Duration) error {
	cmd := valkey.Completed{}

	if ex != nil {
		cmd = c.C.B().Set().Key(key).Value(value).Ex(*ex).Build()
	} else {
		cmd = c.C.B().Set().Key(key).Value(value).Build()
	}

	return c.C.Do(c.Ctx, cmd).Error()
}

func (c *Client) GetString(key string) (string, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Get().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return "", nil
		}

		return "", err
	}

	res, err := resp.ToString()
	if err != nil {
		return "", err
	}

	return res, nil
}

func (c *Client) GetInt64(key string) (int64, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Get().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
	}

	return resp.AsInt64()
}

func (c *Client) AppendSlice(key string, value []string) error {
	return c.C.Do(c.Ctx, c.C.B().Rpush().Key(key).Element(value...).Build()).Error()
}

func (c *Client) AppendSliceReverse(key string, value []string) error {
	return c.C.Do(c.Ctx, c.C.B().Lpush().Key(key).Element(value...).Build()).Error()
}

func (c *Client) GetSlice(key string, start, stop int64) ([]string, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Lrange().Key(key).Start(start).Stop(stop).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return []string{}, nil
		}
		return nil, err
	}

	res, err := resp.AsStrSlice()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) GetSliceByIndex(key string, index int64) (string, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Lindex().Key(key).Index(index).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return "", nil
		}
		return "", err
	}

	res, err := resp.ToString()
	if err != nil {
		return "", err
	}

	return res, nil
}

func (c *Client) GetSliceLen(key string) (int64, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Llen().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
		return 0, err
	}

	res, err := resp.AsInt64()
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (c *Client) SetMap(key, field, value string) error {
	resp := c.C.Do(c.Ctx, c.C.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build())

	return resp.Error()
}

func (c *Client) GetMapStr(key, field string) (string, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Hget().Key(key).Field(field).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return "", nil
		}
		return "", err
	}

	res, err := resp.ToString()
	if err != nil {
		return "", err
	}

	return res, nil
}

func (c *Client) GetMapInt64(key, field string) (int64, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Hget().Key(key).Field(field).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
		return 0, err
	}

	res, err := resp.AsInt64()
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (c *Client) GetMapAll(key string) (map[string]string, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Hgetall().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}

	res, err := resp.AsStrMap()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) GetMapLen(key string) (int64, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Hlen().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
		return 0, err
	}

	res, err := resp.AsInt64()
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (c *Client) CheckIfMapFieldExists(key, field string) (bool, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Hexists().Key(key).Field(field).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return false, nil
		}
		return false, err
	}

	res, err := resp.AsBool()
	if err != nil {
		return false, err
	}

	return res, nil
}

func (c *Client) CheckIfKeyExists(key string) (bool, error) {
	resp := c.C.Do(c.Ctx, c.C.B().Exists().Key(key).Build())

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return false, nil
		}
		return false, err
	}

	res, err := resp.AsBool()
	if err != nil {
		return false, err
	}

	return res, nil
}
