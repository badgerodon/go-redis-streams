package producer

import (
	"context"

	"github.com/go-redis/redis"
)

type config struct {
	stream       string
	maxLen       int64
	maxLenApprox int64
}

// An Option adjusts the config for a producer.
type Option func(*config)

// WithMaxLen sets the max length for a producer.
func WithMaxLen(maxLen int64) Option {
	return func(cfg *config) {
		cfg.maxLen = maxLen
	}
}

// WithMaxLenApprox sets the approximate max length for a producer.
func WithMaxLenApprox(maxLenApprox int64) Option {
	return func(cfg *config) {
		cfg.maxLenApprox = maxLenApprox
	}
}

// A Producer writes messages to a stream.
type Producer struct {
	client *redis.Client
	cfg    *config
}

// New creates a new Producer.
func New(client *redis.Client, stream string, options ...Option) *Producer {
	cfg := &config{
		stream: stream,
	}
	for _, opt := range options {
		opt(cfg)
	}
	return &Producer{
		client: client,
		cfg:    cfg,
	}
}

type writeConfig struct {
	id     string
	values map[string]interface{}
}

// A WriteOption is an option to the write method.
type WriteOption func(*writeConfig)

// WithID sets the id of a write.
func WithID(id string) WriteOption {
	return func(cfg *writeConfig) {
		cfg.id = id
	}
}

// WithField sets a field-value pair in the values for a write.
func WithField(field string, value interface{}) WriteOption {
	return func(cfg *writeConfig) {
		cfg.values[field] = value
	}
}

// Write writes a message to the stream. ID can be set to the empty string (or *) to auto-generate an ID.
func (p *Producer) Write(ctx context.Context, options ...WriteOption) (string, error) {
	cfg := &writeConfig{values: make(map[string]interface{})}
	for _, opt := range options {
		opt(cfg)
	}

	cmd := p.client.WithContext(ctx).XAdd(&redis.XAddArgs{
		Stream:       p.cfg.stream,
		MaxLen:       p.cfg.maxLen,
		MaxLenApprox: p.cfg.maxLenApprox,
		ID:           cfg.id,
		Values:       cfg.values,
	})
	return cmd.Result()
}
