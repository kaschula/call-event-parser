package main_test

import (
	"encoding/csv"
	"errors"
	"os"
	"strings"
	"testing"

	parser "github.com/kaschula/call-event-parser"
	. "github.com/stretchr/testify/assert"
)

const (
	EMPTY_DIR_PATH = "./test/data/emptydir"
	CSV_DATA_PATH  = "./test/data/callevent"
)

func TestAnErrorIsReturnedProcessAlreadyRunning(t *testing.T) {
	createDummyData(t, "test/data/callevent/complete-data.csv")

	p := parser.NewCallEventParser(nil, "./test/data/processed/", "./running")
	runningFile, fErr := os.Create("./running")
	if fErr != nil {
		t.Fatal("Error creating 'running' file")
	}
	defer runningFile.Close()
	defer removeFile("./running")

	err := p.Parse("path to file")

	Error(t, err)
	True(t, strings.Contains("A parsing process is currently running, exiting", err.Error()))
}

func TestAnErrorIsReturnedIfDirectoryDoesNotExist(t *testing.T) {
	createDummyData(t, "test/data/callevent/complete-data.csv")

	p := parser.NewCallEventParser(nil, "./test/data/processed/", "./running")
	err := p.Parse("./path/that/does/not/exist")

	Error(t, err)
	True(t, strings.Contains("Directory does not exist", err.Error()))
}

func TestAnErrorIsReturnedIfNoCSVFilesFoundInDirectory(t *testing.T) {
	p := parser.NewCallEventParser(nil, "./test/data/processed/", "./running")
	err := p.Parse(EMPTY_DIR_PATH)

	Error(t, err)
	True(t, strings.Contains("No CSV files found", err.Error()))
}

func TestAnErrorIsReturnedStoreFailsToPrepare(t *testing.T) {
	createDummyData(t, "test/data/callevent/complete-data.csv")

	store := newStoreStub(errors.New("Store error"), nil)
	p := parser.NewCallEventParser(store, "./test/data/processed/", "./running")
	err := p.Parse(CSV_DATA_PATH)

	Error(t, err)
	True(t, strings.Contains("Store error", err.Error()))
}

func TestCsvDataIsParsedAndStored(t *testing.T) {
	createDummyData(t, "test/data/callevent/complete-data.csv")

	store := newStoreStub(nil, nil)
	expectedData := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2018-01-02 10:27:36", "sale", "4536", "111.00", "GBP"},
		[]string{"2018-01-02 11:28:54", "sale", "6257", "240.49", "GBP"},
		[]string{"2018-01-02 11:48:23", "lead", "5328", "0", "GBP"},
		[]string{"2018-01-02 13:11:17", "sale", "3826", "100.00", "GBP"},
		[]string{"2018-01-02 15:37:42", "sale", "9872", "100.00", "GBP"},
		[]string{"2018-01-02 16:42:12", "lead", "6271", "0", "GBP"},
	}
	expectCallEventFile := parser.CreateCallEventFileFromRaw(expectedData, "test/data/callevent/complete-data.csv")

	p := parser.NewCallEventParser(store, "./test/data/processed/", "./running")
	err := p.Parse(CSV_DATA_PATH)
	defer removeFile("./test/data/processed/complete-data.csv")

	Nil(t, err)
	Equal(t, expectCallEventFile, store.storedData[0])
	True(t, fileExists(t, "./test/data/processed/complete-data.csv"))
	False(t, fileExists(t, "./test/data/callevent/complete-data.csv"))
}

func removeFile(path string) {
	os.Remove(path)
}

func newStoreStub(prepareReturn, createError error) *storeStub {
	return &storeStub{prepareReturn, []parser.CallEventFile{}, createError}
}

type storeStub struct {
	prepareReturn error
	storedData    []parser.CallEventFile
	createError   error
}

func (s *storeStub) Prepare() error {
	return s.prepareReturn
}

func (s *storeStub) Create(file parser.CallEventFile) error {
	if s.createError != nil {
		return s.createError
	}

	s.storedData = append(s.storedData, file)

	return nil
}

func fileExists(t *testing.T, path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}

func createDummyData(t *testing.T, path string) {
	data := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2018-01-02 10:27:36", "sale", "4536", "111.00", "GBP"},
		[]string{"2018-01-02 11:28:54", "sale", "6257", "240.49", "GBP"},
		[]string{"2018-01-02 11:48:23", "lead", "5328", "0", "GBP"},
		[]string{"2018-01-02 13:11:17", "sale", "3826", "100.00", "GBP"},
		[]string{"2018-01-02 15:37:42", "sale", "9872", "100.00", "GBP"},
		[]string{"2018-01-02 16:42:12", "lead", "6271", "0", "GBP"},
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
		t.Fatal("Failed to set up dummy data")
	}

	csvWriter := csv.NewWriter(file)
	csvWriter.WriteAll(data)
	csvWriter.Flush()
}
