package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/akwirick/go-health/Godeps/_workspace/src/github.com/gocql/gocql"
	"github.com/akwirick/go-health/Godeps/_workspace/src/github.com/gorilla/mux"
)

var (
	// Session is the global cassandra session
	Session *gocql.Session
)

func fail(w http.ResponseWriter) {
	http.Error(w, "failed", http.StatusInternalServerError)
}

// Health provides a basic endpoint for determining health
// fails if we can neither start a session or execute a query
func healthHandler(hostport string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		cluster := gocql.NewCluster(hostport)
		cluster.Keyspace = "system"
		cluster.Consistency = gocql.One

		session, sesErr := cluster.CreateSession()
		defer session.Close()

		if sesErr != nil {
			fail(w)
			return
		}

		// Execute test query
		err := session.Query(`SELECT now() FROM system.local`).Exec()
		if err != nil {
			fail(w)
			return
		}

		fmt.Fprintln(w, "success")
	})
}

func main() {
	host := flag.String("host", "127.0.0.1", "Cassandra Host")
	port := flag.Uint("port", 9042, "CQL Port")
	flag.Parse()

	hostport := fmt.Sprintf("%v:%v", *host, *port)

	r := mux.NewRouter()
	r.Handle("/health", healthHandler(hostport))
	fmt.Println("Starting health check on port 8080,  press ctrl-c to exit")
	log.Fatal(http.ListenAndServe(":8080", r))
}
