package kestrel

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"time"
)

type Client struct {
	Timeout    time.Duration
	server     string
	tclient    *KestrelClient
	ttransport *thrift.TFramedTransport
}

func NewClient(server string) *Client {
	return &Client{
		Timeout: 3 * time.Second,
		server:  server,
	}
}

func (c *Client) Get(queueName string, maxItems int32, timeout time.Duration, autoAbort time.Duration) ([]*Item, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return nil, err
	}

	return c.tclient.Get(queueName, maxItems, int32(timeout/time.Millisecond), int32(autoAbort/time.Millisecond))
}

func (c *Client) Put(queueName string, items [][]byte) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	return c.tclient.Put(queueName, items, 0)
}

func (c *Client) Confirm(queueName string, items []*Item) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	ids := make(map[int64]bool, len(items))
	for _, item := range items {
		ids[item.Id] = true
	}

	return c.tclient.Confirm(queueName, ids)
}

func (c *Client) Abort(queueName string, items []*Item) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	ids := make(map[int64]bool, len(items))
	for _, item := range items {
		ids[item.Id] = true
	}

	return c.tclient.Abort(queueName, ids)
}

func (c *Client) FlushAllQueues() error {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return err
	}

	return c.tclient.FlushAllQueues()
}

func (c *Client) Close() {
	if c.ttransport != nil {
		c.ttransport.Close()
	}
	c.ttransport = nil
}

func (c *Client) ensureConnected() error {
	if c.ttransport != nil {
		return nil
	}

	transport, err := thrift.NewTSocketTimeout(c.server, c.Timeout)
	if err == nil {
		err = transport.Open()
		if err == nil {
			c.ttransport = thrift.NewTFramedTransport(transport)
			c.tclient = NewKestrelClientFactory(c.ttransport, thrift.NewTBinaryProtocolFactoryDefault())
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}
