package main

import (
	l "log"
	"log/syslog"
	"os"
)

func main() {
	// setUp
	writer, err := syslog.New(syslog.LOG_NOTICE, "call_event_parser")
	if err == nil {
		l.SetOutput(writer)
	}

	args := os.Args[1:]

	if !validArgs(args) {
		log("In valid command line arguments 9 required")
		os.Exit(0)
	}

	dbUser := args[0]
	dbPassword := args[1]
	dbHost := args[2]
	dbPort := args[3]
	database := args[4]
	table := args[5]
	processedDirectory := args[6]
	uploadDirectory := args[7]
	fileRunningPath := args[8]

	db := NewMySqlEventStore(dbUser, dbPassword, dbHost, dbPort, database, table)
	parser := NewCallEventParser(db, processedDirectory, fileRunningPath)

	parseErr := parser.Parse(uploadDirectory)

	if parseErr != nil {
		log("Call Event Parse Error: " + parseErr.Error())
	}
}

func validArgs(args []string) bool {
	return len(args) == 9
}

func log(message string) {
	l.Print(message)
}
