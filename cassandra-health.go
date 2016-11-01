package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
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

		if sesErr != nil {
			fail(w)
			return
		}

		defer session.Close()

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
	listen_port := flag.Uint("listen_port", 19042, "HTTP listen port")
	flag.Parse()

	hostport := fmt.Sprintf("%v:%v", *host, *port)
	listenport := fmt.Sprintf(":%v", *listen_port)

	r := mux.NewRouter()
	r.Handle("/health", healthHandler(hostport))
	fmt.Println("Starting health check on port", listenport, "press ctrl-c to exit")
	log.Fatal(http.ListenAndServe(listenport, r))
}
