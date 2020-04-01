package kafka

import (
	"context"
	"fmt"
	s "github.com/matang28/go-streams"
	k "github.com/segmentio/kafka-go"
	"time"
)

type kafkaSink struct {
	cfg SinkConfig

	writer *k.Writer
	conn   *k.Conn

	extractor s.KeyExtractor
}

func NewKafkaSink(cfg SinkConfig, extractor s.KeyExtractor) *kafkaSink {
	if extractor == nil {
		extractor = func(entry s.Entry) string {
			return entry.Key
		}
	}

	if cfg.Serializer == nil {
		cfg.Serializer = func(entry s.Entry) []byte {
			return entry.Value.([]byte)
		}
	}

	out := &kafkaSink{cfg: cfg, extractor: extractor}
	s.Log().Info("Connecting to kafka with config: %+v", cfg)
	if err := out.connect(); err != nil {
		panic(err)
	}
	s.Log().Info("Connected to kafka with config: %+v", cfg)
	return out
}

func (this *kafkaSink) Single(entry s.Entry) error {
	return this.writer.WriteMessages(context.Background(), k.Message{
		Key:   []byte(this.extractor(entry)),
		Value: this.cfg.Serializer(entry),
	})
}

func (this *kafkaSink) Batch(entry ...s.Entry) error {
	messages := make([]k.Message, len(entry))
	for idx := range entry {
		messages[idx] = k.Message{
			Key:   []byte(this.extractor(entry[idx])),
			Value: entry[idx].Value.([]byte),
		}
	}
	return this.writer.WriteMessages(context.Background(), messages...)
}

func (this *kafkaSink) Ping() error {
	var err error

	if this.conn == nil {
		for _, host := range this.cfg.Hosts {
			this.conn, err = k.Dial("tcp", host)
			if err != nil {
				continue
			}
		}
	}
	if err != nil {
		return err
	}

	brokers, err := this.conn.Brokers()
	if err != nil {
		return err
	}

	if len(brokers) <= 0 {
		return fmt.Errorf("failed to get a valid list of kafka brokers")
	}

	return nil
}

func (this *kafkaSink) connect() error {
	this.writer = k.NewWriter(k.WriterConfig{
		Brokers:           this.cfg.Hosts,
		Topic:             this.cfg.Topic,
		Balancer:          k.Murmur2Balancer{},
		MaxAttempts:       this.cfg.MaxRetries,
		BatchSize:         this.cfg.BatchSize,
		BatchTimeout:      time.Duration(this.cfg.BatchTimeoutSeconds) * time.Second,
		WriteTimeout:      time.Duration(this.cfg.WriteTimeoutSeconds) * time.Second,
		RebalanceInterval: time.Duration(this.cfg.RebalanceTimeoutSeconds) * time.Second,
		RequiredAcks:      this.cfg.RequiredAcks,
		Async:             this.cfg.Async,
	})

	return this.Ping()
}
