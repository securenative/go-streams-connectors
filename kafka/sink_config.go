package kafka

type SinkConfig struct {
	Hosts []string
	Topic string

	// Limit on how many attempts will be made to deliver a message.
	MaxRetries int

	// Limit on how many messages will be buffered before being sent to a
	// partition.
	BatchSize int

	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	BatchTimeoutSeconds int

	// Timeout for write operation performed by the Writer.
	WriteTimeoutSeconds int

	// This interval defines how often the list of partitions is refreshed from
	// kafka. It allows the Writer to automatically handle when new partitions
	// are added to a topic.
	RebalanceTimeoutSeconds int

	// Number of acknowledges from partition replicas required before receiving
	// a response to a produce request (default to -1, which means to wait for
	// all replicas).
	RequiredAcks int

	// Setting this flag to true causes the WriteMessages method to never block.
	// It also means that errors are ignored since the caller will not receive
	// the returned value. Use this only if you don't care about guarantees of
	// whether the messages were written to kafka.
	Async bool
}

func NewSinkConfig(hosts []string, topic string) SinkConfig {
	var out SinkConfig
	out.Hosts = hosts
	out.Topic = topic
	out.MaxRetries = 10
	out.BatchSize = 100
	out.BatchTimeoutSeconds = 1
	out.WriteTimeoutSeconds = 5
	out.RebalanceTimeoutSeconds = 15
	out.RequiredAcks = -1
	out.Async = false
	return out
}
