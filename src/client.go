package src

import (
	"fmt"

	"github.com/colinmarc/hdfs"
)

type Client struct {
	URL        string
	Port       int
	connection *hdfs.Client
}

func NewClient(url string, port int) (*Client, error) {
	var client Client

	client.URL = url
	client.Port = port

	con, err := hdfs.New(fmt.Sprintf("%s:%d", client.URL, client.Port))
	if err != nil {
		return nil, err
	}

	client.connection = con

	return &client, nil
}

func (c *Client) ReadFile(path string) ([]byte, error) {
	file, err := c.connection.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}
