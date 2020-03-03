package clickhouse

import (
	"fmt"
	"strings"
)

type Config struct {
	Hosts    []string
	Username string
	Password string
	Database string
	Debug    bool
}

func (this *Config) ConnectionString() string {
	if len(this.Hosts) == 0 {
		panic("no valid hosts provided for clickhouse Config, you should set at least one host to connect to")
	}

	base := fmt.Sprintf("tcp://%s?username=%s&password=%s&database=%s&debug=%t",
		this.Hosts[0], this.Username, this.Password, this.Database, this.Debug)

	if len(this.Hosts) > 1 {
		hostsSuffix := fmt.Sprintf("&alt_hosts=%s", strings.Join(this.Hosts[1:], ","))
		base = base + hostsSuffix
	}

	return base
}
