package kafka

import (
	"context"
	"fmt"
	"github.com/matang28/go-streams"
	"github.com/segmentio/kafka-go"
	"os"
	"os/exec"
	"testing"
	"time"
)

var source *kafkaSource
var sink *kafkaSink
var sinkSourceTopic *kafkaSink
var errs go_streams.ErrorChannel
var ch = make(go_streams.EntryChannel, 1000)

func TestMain(m *testing.M) {
	startKafka()

	source = NewKafkaSource(SourceConfig{
		Hosts:         []string{"localhost:9092"},
		Topic:         "test_source",
		ConsumerGroup: "test__source_cg",
	})

	go source.Start(ch, errs)

	sink = NewKafkaSink(SinkConfig{
		Hosts: []string{"localhost:9092"},
		Topic: "test_sink",
	}, func(entry go_streams.Entry) string {
		return entry.Key
	})

	sinkSourceTopic = NewKafkaSink(SinkConfig{
		Hosts: []string{"localhost:9092"},
		Topic: "test_source",
	}, func(entry go_streams.Entry) string {
		return entry.Key
	})

	status := m.Run()

	panicOrPrint(run("docker rm --force snkafka_test || true"))
	os.Exit(status)
}

func readN(n int) []kafka.Message {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test_sink",
	})
	var out []kafka.Message
	for i := 0; i < n; i++ {
		m, err := r.ReadMessage(context.Background())
		panicOnErr(err)
		out = append(out, m)
	}
	return out
}

func startKafka() {
	panicOrPrint(run("docker rm --force snkafka_test || true"))
	panicOrPrint(run("docker run -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost --name snkafka_test landoop/fast-data-dev:latest"))
	for i := 0; i < 20; i++ {
		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "test", 0)
		if err == nil {
			if _, err := conn.Brokers(); err == nil {
				return
			}
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
	panic("could not connect to kafka")
}

func run(cmd string) (string, error) {
	c := exec.Command("/bin/sh", "-c", cmd)
	bytes, err := c.CombinedOutput()
	return string(bytes), err
}

func panicOrPrint(msg string, err error) {
	panicOnErr(err)
	fmt.Printf("%s\n", msg)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
