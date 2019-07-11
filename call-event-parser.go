package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
)

type callEventParser struct {
	processLockFile    string
	processedDirectory string
	store              callEventStore
}

func NewCallEventParser(store callEventStore, processedDirectory, tempFilePath string) callEventParser {
	return callEventParser{tempFilePath, processedDirectory, store}
}

func (p callEventParser) Parse(directoryPath string) error {
	if p.isProcessRunning() {
		return errors.New("A parsing process is currently running, exiting")
	}

	csvFilePaths, err := getCSVFilePaths(directoryPath)
	if err != nil {
		return err
	}

	if err := p.store.Prepare(); err != nil {
		return err
	}

	p.startProcessing()
	defer p.stopProcessing()

	return p.processFiles(csvFilePaths)
}

func (p callEventParser) processFiles(csvFilePaths []string) error {
	for _, filePath := range csvFilePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log(fmt.Sprintf("could not open %#v", filePath))
			continue
		}

		reader := csv.NewReader(file)

		csvFileRecordsRaw := [][]string{}
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log(fmt.Sprintf("Error reading file '%#v' : %#v", filePath, err.Error()))
			}

			csvFileRecordsRaw = append(csvFileRecordsRaw, record)
		}

		callFile := CreateCallEventFileFromRaw(csvFileRecordsRaw, filePath)

		err = p.logFileErrorsAndStoreRecords(callFile)
		if err != nil {
			continue
		}

		p.moveFileToProcessed(callFile)
	}

	return nil
}

func (p callEventParser) startProcessing() error {
	f, err := os.Create(p.processLockFile)
	if f != nil {
		f.Close()
	}

	return err
}

func (p callEventParser) stopProcessing() {
	os.Remove(p.processLockFile)
}

func (p callEventParser) isProcessRunning() bool {
	if _, err := os.Stat(p.processLockFile); os.IsNotExist(err) {
		return false
	}

	return true
}

func (p callEventParser) logFileErrorsAndStoreRecords(processedFile CallEventFile) error {
	for _, message := range processedFile.recordErrors {
		log(message)
	}

	err := p.store.Create(processedFile)
	if err != nil {
		log(err.Error())
		log(fmt.Sprintf("Database write error for file: %#v", processedFile.filenamePath))
	}

	return err
}

func (p callEventParser) moveFileToProcessed(file CallEventFile) error {
	if !directoryExists(p.processedDirectory) {
		err := createDirectory(p.processedDirectory)

		if err != nil {
			return err
		}
	}

	return os.Rename(file.filenamePath, p.processedDirectory+file.GetFilename())
}
