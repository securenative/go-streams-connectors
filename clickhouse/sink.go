package clickhouse

import (
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	s "github.com/matang28/go-streams"
	"strings"
)

type Record []interface{}   // e.g: [15928292, "user1", "GOLD_MEMBER", ["male", "over 40"]]
type RecordMapping []string // e.g: ["timestamp", "id", "type", "tags"]

type clickhouseSink struct {
	cfg        Config
	connection *sql.DB
	mapping    RecordMapping

	table string
	query string
}

func NewClickhouseSink(cfg Config, table string, mapping RecordMapping) *clickhouseSink {
	ch := &clickhouseSink{
		cfg:     cfg,
		mapping: mapping,
		query:   genInsertQuery(table, mapping),
		table:   table,
	}
	s.Log().Info("Connecting to clickhouse: %s", cfg.ConnectionString())
	if err := ch.connect(); err != nil {
		panic(err)
	}
	s.Log().Info("Connected to clickhouse: %s", cfg.ConnectionString())
	return ch
}

func (this *clickhouseSink) Single(entry s.Entry) error {
	s.Log().Warn("Clickhouse isn't optimized to single record writes, please consider using a buffered processor")
	var commitErr error = nil
	for i := 0; i < 3; i++ {
		commitErr = this.commitTx(entry)
		if commitErr == nil {
			return nil
		} else {
			s.Log().Warn("Failed to commit query to clickhouse(%d/3) with error: %s", i, commitErr.Error())
		}
	}
	return commitErr
}

func (this *clickhouseSink) Batch(entry ...s.Entry) error {
	var commitErr error = nil
	for i := 0; i < 3; i++ {
		commitErr = this.commitTx(entry...)
		if commitErr == nil {
			return nil
		} else {
			s.Log().Warn("Failed to commit query to clickhouse(%d/3) with error: %s", i+1, commitErr.Error())
		}
	}
	return commitErr
}

func (this *clickhouseSink) commitTx(entry ...s.Entry) error {
	tx, err := this.connection.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(this.query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	errs := s.NewSinkBatchError()
	for idx := range entry {
		args := entry[idx].Value.(Record)
		if _, e := stmt.Exec(args...); e != nil {
			errs.Add(entry[idx].Key, e)
		}
	}

	if errs.AsError() != nil {
		return errs.AsError()
	}

	crr := tx.Commit()
	if crr != nil {
		if e := tx.Rollback(); e != nil {
			s.Log().Error(e.Error())
		}
	}
	return crr
}

func (this *clickhouseSink) Ping() error {
	if this.connection == nil {
		return fmt.Errorf("cannot ping nil connection")
	}

	return this.connection.Ping()
}

func (this *clickhouseSink) connect() error {
	connection, err := sql.Open("clickhouse", this.cfg.ConnectionString())
	if err != nil {
		return err
	}

	this.connection = connection
	return this.connection.Ping()
}

func genInsertQuery(table string, mapping RecordMapping) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(mapping, ","),
		genPlaceholders(len(mapping)),
	)
}

func genPlaceholders(count int) string {
	var out = make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = "?"
	}
	return strings.Join(out, ",")
}
