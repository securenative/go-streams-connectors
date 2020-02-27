package clickhouse

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

var sink *clickhouseSink

func TestMain(m *testing.M) {
	startClickhouse()

	sink = NewClickhouseSink(Config{
		Hosts:    []string{"localhost:9000"},
		Username: "",
		Password: "",
		Database: "default",
		Debug:    true,
	}, "test", RecordMapping{"timestamp", "name", "likes", "tags"})

	status := m.Run()

	panicOrPrint(run("docker rm --force ch_test || true"))
	os.Exit(status)
}

func createTable() {
	_, err := sink.connection.Exec(`
	CREATE TABLE test
	(
		timestamp       DateTime,
		name            String,
		likes			Int32,
		tags			Array(String)
	)
	ENGINE = Memory;
	`)
	panicOnErr(err)
}

func dropTable() {
	_, err := sink.connection.Exec(`DROP TABLE test`)
	panicOnErr(err)
}

func readAll() []Record {
	var out []Record

	rows, err := sink.connection.Query(`SELECT timestamp, name, likes, tags FROM test`)
	panicOnErr(err)

	for rows.Next() {
		var (
			timestamp time.Time
			name      string
			likes     int32
			tags      []string
		)

		panicOnErr(rows.Scan(&timestamp, &name, &likes, &tags))
		out = append(out, Record{timestamp, name, likes, tags})
	}

	return out
}

func startClickhouse() {
	panicOrPrint(run("docker rm --force ch_test || true"))
	panicOrPrint(run("docker run -d --name ch_test -e ADV_HOST=localhost -p 8123:8123 -p 9000:9000 yandex/clickhouse-server"))
	time.Sleep(5 * time.Second)
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
