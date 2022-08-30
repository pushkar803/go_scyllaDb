package main

//Author: Pushkar Shetye
//step 1: docker run --name some-scylla -d scylladb/scylla

//step 2: docker exec -it some-scylla nodetool status

//Output will be as follows
//Datacenter: datacenter1
//=======================
//Status=Up/Down
//|/ State=Normal/Leaving/Joining/Moving
//--  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
//UN  172.17.0.2  125 KB     256     100.0%            c1906b2b-ce0c-4890-a9d4-8c360f111ad0  rack1

//Note: above output has ip that needs to put in this program on line 25

//step 3: docker exec -it some-scylla cqlsh

//after connecting to cqlsh
//list all keyspaces (same as databases in SQL) : describe keyspaces
//to write queries to specific keyspace: use keyspace_name
//list all tables in keyspace: describe tables
//select * from songs
//if 'use keyspace_name' command is is not executed then u need to specify keyspace name for table in every query
//as select * from go_demo1.songs

import (
	"fmt"
	"time"

	"log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/table"
)

type Song struct {
	ID     gocql.UUID
	Title  string
	Album  string
	Artist string
}

func main() {

	var Keyspace = "go_demo1"
	var Port = 9042
	var Host = "172.17.0.2"
	var tableName = fmt.Sprintf("%s.songs", Keyspace)

	cluster := CreateCluster(1, Port, Host)

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal("Failed to wrap session: ", err)
	}
	log.Println("ScyllaDB Connected Successfully")

	err = CreateKeySpaceIfNotExist(&session, Keyspace)
	if err != nil {
		log.Fatal("create keyspace:", err)
	}

	cluster.Keyspace = Keyspace

	songTable, err := CreateTableIfNotExist(&session, tableName)
	if err != nil {
		log.Fatal("create table:", err)
	}

	err = InsertNewSong(&session, songTable)
	if err != nil {
		log.Fatal("Insert ExecRelease() failed:", err)
	}

	items, err := GetSong(&session, songTable)
	if err != nil {
		log.Fatal("Get ExecRelease() failed:", err)
	}
	for _, i := range items {
		log.Printf("%+v", *i)
	}

	defer session.Close()

}

func CreateKeySpaceIfNotExist(session *gocqlx.Session, Keyspace string) error {
	return session.ExecStmt(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, Keyspace))
}

func CreateTableIfNotExist(session *gocqlx.Session, tableName string) (*table.Table, error) {
	err := session.ExecStmt(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id uuid,
		title text,
		album text, 
		artist text,
		PRIMARY KEY (id, title, album, artist))`, tableName))

	songMetadata := table.Metadata{
		Name:    tableName,
		Columns: []string{"id", "title", "album", "artist"},
		PartKey: []string{"id"},
		SortKey: []string{"title", "album", "artist"},
	}

	songTable := table.New(songMetadata)
	return songTable, err
}

func InsertNewSong(session *gocqlx.Session, songTable *table.Table) error {
	insertSong := songTable.InsertQuery(*session)
	insertSong.BindStruct(Song{
		ID:     mustParseUUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
		Title:  "Lost In Love Mashup | Incomplete love - Emotional Mashup",
		Album:  "AB Ambients Chillout",
		Artist: "Sunix Thakur",
	})

	return insertSong.ExecRelease()
}

func GetSong(session *gocqlx.Session, songTable *table.Table) ([]*Song, error) {

	querySong := songTable.SelectQuery(*session)
	querySong.BindStruct(&Song{
		ID: mustParseUUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
	})

	var items []*Song
	err := querySong.Select(&items)
	return items, err
}

func CreateCluster(consistency gocql.Consistency, port int, hosts ...string) *gocql.ClusterConfig {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster := gocql.NewCluster(hosts...)
	const timeout = 5 * time.Second
	cluster.Timeout = timeout
	cluster.RetryPolicy = retryPolicy
	cluster.Consistency = consistency
	cluster.Port = port
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cluster
}

func mustParseUUID(s string) gocql.UUID {
	u, err := gocql.ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}
