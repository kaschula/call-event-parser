package main_test

import (
	"strings"
	"testing"

	parser "github.com/kaschula/call-event-parser"
	. "github.com/stretchr/testify/assert"
)

func TestItReturnsAnErrorWhenNoEventDateTimeGiven(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{""},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'eventDatetime' value"))
}

func TestItReturnsAnErrorWhenDateFormatIsIncorrect(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12-01-30"},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "date format must be yyyy-mm-dd hh:mm:ss"))
}

func TestItReturnsAnErrorWhenEventActionIsMissing(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", ""},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'eventAction' value"))
}

func TestItReturnsAnErrorWhenEventActionIsMoreThan20CharactersLong(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "abcdefghijklmnopqrstu"},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'eventAction' to be between 1 - 20 in length"))
}

func TestItReturnsAnErrorWhenCallRefIsMissing(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", ""},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'callRef' value"))
}

func TestItReturnsAnErrorWhenCallRefIsNotAValidInteger(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", "notAnInt"},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'callRef' to be a valid integer"))
}

func TestItReturnsAnErrorWhenEventValueIsNoteAValidFloat(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", "1234", "2.1a"},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'eventValue' to be a valid float"))
}

func TestItReturnsAnErrorWhenEventCurrencyCodeIsNotSetAndEventValueIsMoreThanZero(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", "1234", "1.0", ""},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.ValidData()))
	True(t, strings.Contains(callFile.RecordErrors()[0], "requires 'eventCurrencyCode' if event value is more than 0.0"))
}

func TestItDoesNotReturnAnErrorWhenEventValueIsZeroAndNoCurrencyCodeSet(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", "1234", "0.0", ""},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 0, len(callFile.RecordErrors()))
	Equal(t, 1, len(callFile.ValidData()))
}

func TestItReturnsItSetsTheEventValueToZeroFloatIfEmpty(t *testing.T) {
	rawCsv := [][]string{
		[]string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"},
		[]string{"2012-01-02 12:01:30", "sale", "1234", "", "GBP"},
	}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file")

	Equal(t, 1, len(callFile.ValidData()))
	Equal(t, "0.00", callFile.ValidData()[0][parser.COL_EVENT_VAL])
}

func TestItReturnsTheFileName(t *testing.T) {
	rawCsv := [][]string{}

	callFile := parser.CreateCallEventFileFromRaw(rawCsv, "path/to/file.txt")

	Equal(t, "file.txt", callFile.GetFilename())
}
