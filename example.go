package cqldao

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

const CassandraHost = "localhost"
const CassandraKeyspace = "test_lab"
const TestTableName = "gocql_timestamp_test"

func connectToCassandra() (*gocql.Session, error) {
	cluster := gocql.NewCluster(CassandraHost)
	cluster.Keyspace = CassandraKeyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.ProtoVersion = 3
	cluster.DefaultTimestamp = true // it is already true by default
	return cluster.CreateSession()
}

func createDatabase(session *gocql.Session) error {

	removeTableStmt := fmt.Sprintf("DROP TABLE IF EXISTS %v", TestTableName)

	createTableStmt := fmt.Sprintf(`CREATE TABLE %v (
		id bigint,
		a text,
		b text,
		timestamp bigint,
		PRIMARY KEY (id)
	)
	WITH COMPACTION = {'class' : 'LeveledCompactionStrategy'}`,
		TestTableName)

	// Create table
	if err := session.Query(removeTableStmt).Exec(); err != nil {
		return err
	}

	if err := session.Query(createTableStmt).Exec(); err != nil {
		return err
	}

	return nil
}

func checkTimestamp(session *gocql.Session, timestamp int64, id1 int64, id2 int64) error {

	selectStmt := fmt.Sprintf(`select timestamp, writetime(a) as ta, writetime(b) as tb
        from %v where id in (?, ?)`, TestTableName)

	var writtenTimestamp int64
	var ta int64
	var tb int64

	it := session.Query(selectStmt, id1, id2).Iter()

	for it.Scan(&writtenTimestamp, &ta, &tb) {
		if writtenTimestamp != timestamp || ta != timestamp || tb != timestamp {
			return errors.New("Read timestamp doesn't match written timestamp")
		}
	}

	if err := it.Close(); err != nil {
		return err
	}

	return nil
}

func TestGOCQL_CreateDatabase(t *testing.T) {
	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Create database
	if err := createDatabase(session); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Query_WithTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?)", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	q1 := session.Query(insertStmt, 1, "foo 1", "bar 1", customTimestamp).WithTimestamp(customTimestamp)
	q2 := session.Query(insertStmt, 2, "foo 2", "bar 2", customTimestamp).WithTimestamp(customTimestamp)

	if err := q1.Exec(); err != nil {
		t.Fatal(err)
	}

	if err := q2.Exec(); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 1, 2); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Query_UsingTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	q1 := session.Query(insertStmt, 3, "foo 3", "bar 3", customTimestamp, customTimestamp)
	q2 := session.Query(insertStmt, 4, "foo 4", "bar 4", customTimestamp, customTimestamp)

	if err := q1.Exec(); err != nil {
		t.Fatal(err)
	}

	if err := q2.Exec(); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 3, 4); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Batch_Logged_WithTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?)", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	batch := session.NewBatch(gocql.LoggedBatch).WithTimestamp(customTimestamp)
	batch.Query(insertStmt, 5, "foo 5", "bar 5", customTimestamp)
	batch.Query(insertStmt, 6, "foo 6", "bar 6", customTimestamp)

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 5, 6); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Batch_Unlogged_WithTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?)", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	batch := session.NewBatch(gocql.UnloggedBatch).WithTimestamp(customTimestamp)
	batch.Query(insertStmt, 7, "foo 7", "bar 7", customTimestamp)
	batch.Query(insertStmt, 8, "foo 8", "bar 8", customTimestamp)

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 7, 8); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Batch_Logged_UsingTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query(insertStmt, 9, "foo 9", "bar 9", customTimestamp, customTimestamp)
	batch.Query(insertStmt, 10, "foo 10", "bar 10", customTimestamp, customTimestamp)

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 9, 10); err != nil {
		t.Fatal(err)
	}
}

func TestGOCQL_Batch_Unlogged_UsingTimestamp(t *testing.T) {

	insertStmt := fmt.Sprintf("INSERT INTO %v (id, a, b, timestamp) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", TestTableName)

	// Connect to Cassandra
	session, err := connectToCassandra()
	if err != nil {
		t.Fatal(err)
	}

	// Execute test
	customTimestamp := time.Now().UnixNano() / 1000
	batch := session.NewBatch(gocql.UnloggedBatch)
	batch.Query(insertStmt, 11, "foo 11", "bar 11", customTimestamp, customTimestamp)
	batch.Query(insertStmt, 12, "foo 12", "bar 12", customTimestamp, customTimestamp)

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Check
	if err := checkTimestamp(session, customTimestamp, 11, 12); err != nil {
		t.Fatal(err)
	}
}
