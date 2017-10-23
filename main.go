package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

/*
cqlsh> create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
cqlsh> create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
cqlsh> create index on example.tweet(timeline);
cqlsh> use example ;
cqlsh:example> select * from tweet ;

*/
func main() {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("error creating session")
		return
	}
	defer session.Close()

	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), "hello world").Exec(); err != nil {
		log.Fatal("error in insert", err)
	}

	var id gocql.UUID
	var text string

	/* Search for a specific set of records whose 'timeline' column matches
	 * the value 'me'. The secondary index that we created earlier will be
	 * used for optimizing the search */
	if err := session.Query(`SELECT id, text FROM tweet WHERE timeline =? LIMIT 1`,
		"me").Consistency(gocql.One).Scan(&id, &text); err != nil {
		log.Fatal("error in query", err)
	}
	fmt.Println("Tweet:", id, text)
	iter := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`, "me").Iter()
	for iter.Scan(&id, &text) {
		fmt.Println("Tweet:", id, text)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

}
