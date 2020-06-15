module github.com/securenative/go-streams-connectors/clickhouse

go 1.11

require (
	github.com/ClickHouse/clickhouse-go v0.0.0
	github.com/matang28/go-streams v0.0.0-20200228083127-b9cf444c3666
	github.com/stretchr/testify v1.5.1
)

// todo: remove replace and take version from ClickHouse/clickhouse-go and not the fork when they will release a version after 1.4.0
replace github.com/ClickHouse/clickhouse-go => github.com/securenative/clickhouse-go v1.4.1
