package kafka

import (
	"fmt"
	s "github.com/matang28/go-streams"
	k "github.com/segmentio/kafka-go"
)

type SourceConfig struct {
	Hosts []string
	Topic string

	// GroupID holds the optional consumer group id.  If GroupID is specified, then
	// Partition should NOT be specified e.g. 0
	ConsumerGroup string

	// The capacity of the internal message queue, defaults to 100 if none is
	// set.
	QueueCapacity int

	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	MaxWaitSeconds int

	// ReadLagInterval sets the frequency at which the reader lag is updated.
	// Setting this field to a negative value disables lag reporting.
	ReadLagIntervalSec int

	// HeartbeatInterval sets the optional frequency at which the reader sends the consumer
	// group heartbeat update.
	//
	// Default: 3s
	//
	// Only used when GroupID is set
	HeartbeatIntervalSec int

	// CommitInterval indicates the interval at which offsets are committed to
	// the broker.  If 0, commits will be handled synchronously.
	//
	// Default: 1 second
	//
	// Only used when GroupID is set
	CommitIntervalMs int

	// PartitionWatchInterval indicates how often a reader checks for partition changes.
	// If a reader sees a partition change (such as a partition add) it will rebalance the group
	// picking up new partitions.
	//
	// Default: 5s
	//
	// Only used when GroupID is set and WatchPartitionChanges is set.
	PartitionWatchIntervalSec int

	// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
	// polling the brokers and rebalancing if any partition changes happen to the topic.
	WatchPartitionChanges bool

	// SessionTimeout optionally sets the length of time that may pass without a heartbeat
	// before the coordinator considers the consumer dead and initiates a rebalance.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	SessionTimeoutSec int

	// RebalanceTimeout optionally sets the length of time the coordinator will wait
	// for members to join as part of a rebalance.  For kafka servers under higher
	// load, it may be useful to set this value higher.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	RebalanceTimeoutSec int

	// JoinGroupBackoff optionally sets the length of time to wait between re-joining
	// the consumer group after an error.
	//
	// Default: 5s
	JoinGroupBackoffSec int

	// StartOffset determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset.  If
	// non-zero, it must be set to one of FirstOffset or LastOffset.
	//
	// Default: FirstOffset
	//
	// Only used when GroupID is set
	StartOffset int64

	// BackoffDelayMin optionally sets the smallest amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 100ms
	ReadBackoffMinMs int

	// BackoffDelayMax optionally sets the maximum amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 1s
	ReadBackoffMaxMs int

	// Limit of how many attempts will be made before delivering the error.
	//
	// The default is to try 5 times.
	MaxAttempts int

	// Enables the caller to choose what the output entry will be after the message was received.
	//
	// The default is ValueEntryFunc to preserve backward computability
	ValueExtractor ValueExtractorFunc
}

func NewSourceConfig(hosts []string, topic string, consumerGroupId string) SourceConfig {
	var out SourceConfig
	out.Hosts = hosts
	out.Topic = topic
	out.ConsumerGroup = consumerGroupId
	out.QueueCapacity = 100
	out.MaxWaitSeconds = 10
	out.ReadLagIntervalSec = 60
	out.HeartbeatIntervalSec = 3
	out.CommitIntervalMs = 1000
	out.PartitionWatchIntervalSec = 5
	out.WatchPartitionChanges = true
	out.SessionTimeoutSec = 30
	out.RebalanceTimeoutSec = 30
	out.JoinGroupBackoffSec = 5
	out.StartOffset = k.LastOffset
	out.ReadBackoffMinMs = 100
	out.ReadBackoffMaxMs = 1000
	out.MaxAttempts = 5
	out.ValueExtractor = ValueEntryFunc
	return out
}

// define the OutputEntryFunc options
type ValueExtractorFunc = func(k.Message) s.Entry

var (
	// default
	ValueEntryFunc = func(m k.Message) s.Entry {
		return s.Entry{
			Key:   fmt.Sprintf("%d-%s", m.Offset, m.Key),
			Value: m.Value,
		}
	}

	// can provide all the message parameters when needed
	MessageEntryFunc = func(m k.Message) s.Entry {
		return s.Entry{
			Key:   fmt.Sprintf("%d-%s", m.Offset, m.Key),
			Value: m,
		}
	}
)
