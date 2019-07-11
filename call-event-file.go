package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	DATE_TIME_FORMAT        = "2006-02-01 15:04:05"
	COL_EVENT_DATETIME      = 0
	COL_EVENT_ACTION        = 1
	COL_CALL_REF            = 2
	COL_EVENT_VAL           = 3
	COL_EVENT_CURRENCY_CODE = 4
)

type CallEventFile struct {
	filenamePath    string
	validData       [][]string
	recordErrors    []string
	numberOfRecords int
}

func (f *CallEventFile) GetFilename() string {
	parts := strings.Split(f.filenamePath, "/")

	return parts[len(parts)-1:][0]
}

func (f *CallEventFile) RecordErrors() []string {
	return f.recordErrors
}

func (f *CallEventFile) ValidData() [][]string {
	return f.validData
}

func CreateCallEventFileFromRaw(rawData [][]string, filepath string) CallEventFile {
	data := removeHeader(rawData)
	validRecords := [][]string{}
	errorRecords := []string{}

	for i, row := range data {
		// COL_EVENT_DATETIME
		eventDateTimeValue := row[COL_EVENT_DATETIME]

		if eventDateTimeValue == "" {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'eventDatetime' value", filepath, i+1),
			)
			continue
		}

		_, err := time.Parse(DATE_TIME_FORMAT, eventDateTimeValue)
		if err != nil {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v date format must be yyyy-mm-dd hh:mm:ss", filepath, i+1),
			)
			continue
		}

		// COL_EVENT_ACTION
		if row[COL_EVENT_ACTION] == "" {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'eventAction' value", filepath, i+1),
			)
			continue
		}

		if l := len(row[COL_EVENT_ACTION]); l == 0 || l > 20 {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'eventAction' to be between 1 - 20 in length", filepath, i+1),
			)
			continue
		}

		// COL_CALL_REF
		if row[COL_CALL_REF] == "" {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'callRef' value", filepath, i+1),
			)
			continue
		}

		if _, err := strconv.ParseInt(row[COL_CALL_REF], 10, 64); err != nil {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'callRef' to be a valid integer", filepath, i+1),
			)
			continue
		}

		// COL_EVENT_VAL
		if row[COL_EVENT_VAL] == "" {
			row[COL_EVENT_VAL] = "0.00"
		}

		eventValue, err := strconv.ParseFloat(row[COL_EVENT_VAL], 64)

		if err != nil {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'eventValue' to be a valid float", filepath, i+1),
			)
			continue
		}

		// COL_EVENT_CURRENCY_CODE
		if row[COL_EVENT_CURRENCY_CODE] == "" && eventValue > 0.0 {
			errorRecords = append(
				errorRecords,
				fmt.Sprintf("File: %#v. Record: %#v requires 'eventCurrencyCode' if event value is more than 0.0", filepath, i+1),
			)
			continue
		}

		validRecords = append(validRecords, row)
	}

	return CallEventFile{filepath, validRecords, errorRecords, len(data)}
}

func removeHeader(rawCsv [][]string) [][]string {
	if len(rawCsv) > 0 {
		return rawCsv[1:]
	}

	return rawCsv
}
