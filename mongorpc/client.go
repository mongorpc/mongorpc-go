// Package mongorpc provides a Go client for MongoRPC servers.
//
// Example usage:
//
//	client, err := mongorpc.NewClient("localhost:50051")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	users := client.Database("mydb").Collection("users")
//	doc, err := users.FindByID(ctx, "user-id")
package mongorpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client is a MongoRPC client.
type Client struct {
	conn    *grpc.ClientConn
	address string
	options *ClientOptions
}

// ClientOptions configures the client.
type ClientOptions struct {
	// API key for authentication
	APIKey string
	// JWT token for authentication
	Token string
	// Connection timeout
	Timeout time.Duration
	// Use TLS
	Secure bool
}

// NewClient creates a new MongoRPC client.
func NewClient(address string, opts ...ClientOption) (*Client, error) {
	options := &ClientOptions{
		Timeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		address: address,
		options: options,
	}, nil
}

// ClientOption configures the client.
type ClientOption func(*ClientOptions)

// WithAPIKey sets the API key for authentication.
func WithAPIKey(key string) ClientOption {
	return func(o *ClientOptions) {
		o.APIKey = key
	}
}

// WithToken sets the JWT token for authentication.
func WithToken(token string) ClientOption {
	return func(o *ClientOptions) {
		o.Token = token
	}
}

// WithTimeout sets the connection timeout.
func WithTimeout(d time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.Timeout = d
	}
}

// Database returns a database handle.
func (c *Client) Database(name string) *Database {
	return &Database{
		client: c,
		name:   name,
	}
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// authContext adds authentication metadata to context.
func (c *Client) authContext(ctx context.Context) context.Context {
	if c.options.APIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", c.options.APIKey)
	}
	if c.options.Token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.options.Token)
	}
	return ctx
}
