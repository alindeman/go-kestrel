package kestrel

import (
	"./kthrift"
	"git.apache.org/thrift.git/lib/go/thrift"
	"time"
)

type Client struct {
	Timeout           time.Duration
	CommandsPerServer int

	servers                     []string
	serverIndex                 int
	commandsSentToCurrentServer int

	tclient    *kthrift.KestrelClient
	ttransport *thrift.TFramedTransport
}

func NewClient(servers ...string) *Client {
	return &Client{
		// defaults
		Timeout:           3 * time.Second,
		CommandsPerServer: 10000,

		servers:     servers,
		serverIndex: len(servers) - 1,
	}
}

func (c *Client) Get(queueName string, maxItems int32, timeout time.Duration, autoAbort time.Duration) ([]*kthrift.Item, error) {
	err := c.connectToNextServerIfNeeded()
	if err != nil {
		return nil, err
	}

	return c.tclient.Get(queueName, maxItems, int32(timeout/time.Millisecond), int32(autoAbort/time.Millisecond))
}

func (c *Client) Put(queueName string, items ...[]byte) (int32, error) {
	err := c.connectToNextServerIfNeeded()
	if err != nil {
		return 0, err
	}

	return c.tclient.Put(queueName, items, 0)
}

func (c *Client) connectToNextServer() error {
	c.serverIndex = (c.serverIndex + 1) % len(c.servers)

	if c.ttransport != nil {
		c.ttransport.Close()
	}
	c.ttransport = nil

	transport, err := thrift.NewTSocketTimeout(c.servers[c.serverIndex], c.Timeout)
	if err == nil {
		err := transport.Open()
		if err == nil {
			c.ttransport = thrift.NewTFramedTransport(transport)
			c.tclient = kthrift.NewKestrelClientFactory(c.ttransport, thrift.NewTBinaryProtocolFactoryDefault())
			c.commandsSentToCurrentServer = 0
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (c *Client) connectToNextServerIfNeeded() error {
	if c.tclient == nil || c.commandsSentToCurrentServer >= c.CommandsPerServer {
		return c.connectToNextServer()
	} else {
		return nil
	}
}
