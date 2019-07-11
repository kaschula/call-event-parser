# Call Event Parser

## Introduction

This application processes sets of call data in a specific CSV format. The data is read, validated and then uploaded to a database. Once a file has been successfully processed it moved to a process directory. Any records that can not be processed form files are logged to the syslog.

This project is designed to be run as a back ground task.

## Config

The project is a CLI application and requires 9 arguments to be run. The variables that can be configured are:

- Database Username
- Database Password
- Database Host
- Database Port
- Database Name
- Database Table
- Process directory path (Where files will be moved when processed)
- Upload directory path (Directory where incoming cvs files are located)
- File running path (Path to a temport file that stops this application running in more than one process at a time)


## Build and Run

To build this project run the following in the root of the project

`go install`

from you $GOPATH/bin directory run the application passingin the arguments

`./call-event-parser username password host port db_name db_table "./processed/" "./uploaded" "/tmp/call_event_parse_running"`


### Things to note

The processed directory argument must include a '/' at the end. This is a bug that needs to be resolved. Make sure the application as read/write access to the `File running path` location.


## Tests

This project contains test which can be run from the project root using 

`go test ./...`