package main

import (
	"net/http"
)

func main() {
	fs := http.FileServer(http.Dir("frontend"))
	http.Handle("/", fs)
	http.HandleFunc("/messages", HelloServer)
	for {
		http.ListenAndServe(":5000", nil)
		// error handling, etc..
	}
}

func HelloServer(w http.ResponseWriter, r *http.Request) {
	// fmt.Fprintf(w, "Hello, %s!", r.URL.Path[1:])
}
