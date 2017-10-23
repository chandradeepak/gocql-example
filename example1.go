package main1

import (
   "github.com/gocql/gocql"
   "log"
   "time"
)

func PerformOperations() {
   // Provide the cassandra cluster instance here.
   cluster := gocql.NewCluster("127.0.0.1")

   // The authenticator is needed if password authentication is
   // enabled for your Cassandra installation. If not, this can
   // be removed.
   cluster.Authenticator = gocql.PasswordAuthenticator{
	   Username: "some_username",
	   Password: "some_password",
   }

   // gocql requires the keyspace to be provided before the session is created.
   // In future there might be provisions to do this later.
   cluster.Keyspace = "keyspace_name"

   // This is time after which the creation of session call would timeout.
   // This can be customised as needed.
   cluster.Timeout = 5 * time.Second

   cluster.ProtoVersion = 4
   session, err := cluster.CreateSession()
   if err != nil {
	   log.Fatalf("Could not connect to cassandra cluster: %v", err)
   }

   // Check if the table already exists. Create if table does not exist
   keySpaceMeta, _ := session.KeyspaceMetadata("keyspace_name")

   if _, exists := keySpaceMeta.Tables["person"]; exists != true {
	   // Create a table
	   session.Query("CREATE TABLE person (" +
		   "id text, name text, phone text, " +
		   "PRIMARY KEY (id))").Exec()
   }

   // DIY: Update table with something if it already exist.

   // Insert record into table using prepared statements
   session.Query("INSERT INTO person (id, name, phone) VALUES (?, ?, ?)",
	   "shalabh", "Shalabh Aggarwal", "1234567890").Exec()

   // DIY: Update existing record

   // Select record and run some process on data fetched
   var name string
   var phone string
   if err := session.Query(
	   "SELECT name, phone FROM person WHERE id='shalabh'").Scan(
	   &name, &phone); err != nil {
	   if err != gocql.ErrNotFound {
		   log.Fatalf("Query failed: %v", err)
	   }
   }
   log.Printf("Name: %v", name)
   log.Printf("Phone: %v", phone)

   // Fetch multiple rows and run process over them
   iter := session.Query("SELECT name, phone FROM person").Iter()
   for iter.Scan(&name, &phone) {
	   log.Printf("Iter Name: %v", name)
	   log.Printf("Iter Phone: %v", phone)
   }

   // DIY: Delete record
}

func main() {
   PerformOperations()
}