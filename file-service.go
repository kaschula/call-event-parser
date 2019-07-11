package main

import (
	"errors"
	"os"
	"path/filepath"
)

func directoryExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}

func getCSVFilePaths(directory string) ([]string, error) {
	if !directoryExists(directory) {
		return nil, errors.New("Directory does not exist")
	}

	csvFiles := []string{}
	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".csv" {
			csvFiles = append(csvFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(csvFiles) == 0 {
		return nil, errors.New("No CSV files found")
	}

	return csvFiles, nil
}

func createDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}
