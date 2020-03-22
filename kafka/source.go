package kafka

import (
	"context"
	"fmt"
	s "github.com/matang28/go-streams"
	k "github.com/segmentio/kafka-go"
	"io"
	"net"
	"sync"
	"time"
)

const kafkaSourceName = "kafkaSource"

type kafkaSource struct {
	name   string
	cfg    SourceConfig
	reader *k.Reader
	conn   *k.Conn

	uncommittedMessages map[string]k.Message
	closeCh             chan bool
	mutex               sync.Mutex
}

func NewKafkaSource(cfg SourceConfig) *kafkaSource {
	name := fmt.Sprintf("%s-%d", kafkaSourceName, time.Now().UnixNano())
	return &kafkaSource{
		cfg:                 cfg,
		name:                name,
		uncommittedMessages: make(map[string]k.Message),
		closeCh:             make(chan bool),
		mutex:               sync.Mutex{},
	}
}

func (this *kafkaSource) Start(channel s.EntryChannel, errorChannel s.ErrorChannel) {
	s.Log().Info("Connecting to kafka with config: %+v", this.cfg)
	if err := this.connect(); err != nil {
		panic(err)
	}
	s.Log().Info("Connected to kafka with config: %+v", this.cfg)

	defer func() {
		s.Log().Info("Disconnecting from kafka with config: %+v", this.cfg)
		errorChannel <- s.NewEofError(this)
		if err := this.disconnect(); err != nil {
			panic(err)
		}
		s.Log().Info("Disconnected from kafka with config: %+v", this.cfg)
	}()

loop:
	for {
		select {
		case <-this.closeCh:
			close(channel)
			break loop
		default:
			m, err := this.reader.FetchMessage(context.Background())
			if err != nil {
				handleError(err, errorChannel)
			} else {
				entry := this.cfg.ValueExtractor(m)
				this.mutex.Lock()
				this.uncommittedMessages[entry.Key] = m
				this.mutex.Unlock()
				channel <- entry
			}
		}
	}
}

func handleError(e error, channel s.ErrorChannel) {
	if e != nil {
		if e == io.EOF {
			panic(e)
		}

		_, ok := e.(*net.OpError)
		if ok {
			panic(e)
		}

		channel <- e
	}
}

func (this *kafkaSource) Stop() error {
	this.closeCh <- true
	return nil
}

func (this *kafkaSource) CommitEntry(keys ...string) error {
	messages := make([]k.Message, len(keys))
	for _, key := range keys {
		m, found := this.uncommittedMessages[key]
		if found {
			messages = append(messages, m)
		}
		this.mutex.Lock()
		delete(this.uncommittedMessages, key)
		this.mutex.Unlock()
	}
	return this.reader.CommitMessages(context.Background(), messages...)
}

func (this *kafkaSource) Name() string {
	return this.name
}

func (this *kafkaSource) Ping() error {
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

func (this *kafkaSource) connect() error {
	this.reader = k.NewReader(k.ReaderConfig{
		Brokers:                this.cfg.Hosts,
		Topic:                  this.cfg.Topic,
		GroupID:                this.cfg.ConsumerGroup,
		QueueCapacity:          this.cfg.QueueCapacity,
		MaxWait:                time.Duration(this.cfg.MaxWaitSeconds) * time.Second,
		ReadLagInterval:        time.Duration(this.cfg.ReadLagIntervalSec) * time.Second,
		HeartbeatInterval:      time.Duration(this.cfg.HeartbeatIntervalSec) * time.Second,
		CommitInterval:         time.Duration(this.cfg.CommitIntervalMs) * time.Millisecond,
		PartitionWatchInterval: time.Duration(this.cfg.PartitionWatchIntervalSec) * time.Second,
		WatchPartitionChanges:  this.cfg.WatchPartitionChanges,
		SessionTimeout:         time.Duration(this.cfg.SessionTimeoutSec) * time.Second,
		RebalanceTimeout:       time.Duration(this.cfg.RebalanceTimeoutSec) * time.Second,
		JoinGroupBackoff:       time.Duration(this.cfg.JoinGroupBackoffSec) * time.Second,
		StartOffset:            this.cfg.StartOffset,
		ReadBackoffMin:         time.Duration(this.cfg.ReadBackoffMinMs) * time.Millisecond,
		ReadBackoffMax:         time.Duration(this.cfg.ReadBackoffMaxMs) * time.Millisecond,
		MaxAttempts:            this.cfg.MaxAttempts,
	})

	err := this.Ping()
	return err
}

func (this *kafkaSource) disconnect() error {
	if this.reader != nil {
		if err := this.reader.Close(); err != nil {
			return err
		}
	}

	if this.conn != nil {
		if err := this.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
