package couchbase

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

var testConfig = NewSinkConfig("couchbase://localhost", "Administrator", "123456", "", "test")
var sink *couchbaseSink
var probe = couchbaseSink{config: testConfig}

func TestMain(m *testing.M) {
	startCouchbase()
	sink = NewCouchbaseSink(testConfig)
	status := m.Run()
	panicOrPrint(run("docker rm --force cb_test || true"))
	os.Exit(status)
}

func startCouchbase() {
	panicOrPrint(run("docker rm --force cb_test || true"))
	panicOrPrint(run("cd docker && docker build -t sncb ."))
	panicOrPrint(run("docker run -d --name cb_test -p 8091-8094:8091-8094 -p 11210:11210 sncb"))

	for i := 0; i < 20; i++ {
		if err := probe.connect(); err == nil {
			return
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
	time.Sleep(2 * time.Second)
	panic("could not connect to couchbase")
}

func read(key string, modelPtr interface{}) {
	_, err := probe.bucket.Get(key, modelPtr)
	panicOnErr(err)
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
