package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type inputFile struct {
	// struct to hold cli arguements
	filepath  string
	separator string
	pretty    bool
}

func exitGracefully(err error) {
	// error handling function to carefully manage user error.
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func check(e error) {
	// conditional statement to handle errors prior to exit.
	if e != nil {
		exitGracefully(e)
	}
}

func getFileData() (inputFile, error) {
	// Validate arguments have correct length
	if len(os.Args) < 2 {
		return inputFile{}, errors.New("A filepath argument is required")
	}
	// default seperator is a comma but can take semi colon also from csv.
	separator := flag.String("separator", "comma", "Column separator")
	pretty := flag.Bool("pretty", false, "Generate pretty JSON")
	// parse flag arguements
	flag.Parse()
	// filepath arguement in position zero.
	fileLocation := flag.Arg(0)

	// currently only take commas and semi colon.
	if !(*separator == "comma" || *separator == "semicolon") {
		return inputFile{}, errors.New("Only comma or semicolon separators are allowed")
	}
	// populate struct with values from command line.
	return inputFile{fileLocation, *separator, *pretty}, nil
}

func checkIfValidFile(filename string) (bool, error) {
	// Check if file is CSV
	if fileExtension := filepath.Ext(filename); fileExtension != ".csv" {
		return false, fmt.Errorf("File %s is not CSV", filename)
	}

	// Check if file does exist
	if _, err := os.Stat(filename); err != nil && os.IsNotExist(err) {
		return false, fmt.Errorf("File %s does not exist", filename)
	}

	return true, nil
}

func processLine(headers []string, dataList []string) (map[string]string, error) {
	// if given line delimiter value length is not the length of inital header
	if len(dataList) != len(headers) {
		// throw error as not a valid record.
		return nil, errors.New("Line doesn't match headers format. Skipping")
	}

	recordMap := make(map[string]string)

	for i, name := range headers {
		recordMap[name] = dataList[i]
	}

	return recordMap, nil
}

func processCsvFile(fileData inputFile, writerChannel chan<- map[string]string) {
	// get file from OS
	file, err := os.Open(fileData.filepath)
	// Check for error
	check(err)
	// close the file now we have data in memory
	defer file.Close()
	// Get Headers
	var headers, line []string
	// read data to reader
	reader := csv.NewReader(file)
	// from struct, read separator and assign to reader.
	// default is comma, no need to explictly define.
	if fileData.separator == "semicolon" {
		reader.Comma = ';'
	}
	// read values from reader, throw error if there otherwise nil.
	// this reads the first line in reader, following lines are
	// assumed to be values.
	headers, err = reader.Read()
	check(err)
	// for each line in reader, process check the line is valid and add to record map
	for {
		line, err = reader.Read()
		// if end of CSV close writer and exit function.
		if err == io.EOF {
			close(writerChannel)
			break
		} else if err != nil {
			// if error is not null then call exit func.
			exitGracefully(err)
		}

		record, err := processLine(headers, line)

		if err != nil {
			fmt.Printf("Line: %sError: %s\n", line, err)
			continue
		}

		writerChannel <- record
	}
}

func createStringWriter(csvPath string) func(string, bool) {
	// get path from inital CSV
	jsonDir := filepath.Dir(csvPath)
	//
	jsonName := fmt.Sprintf("%s.json", strings.TrimSuffix(filepath.Base(csvPath), ".csv"))
	finalLocation := fmt.Sprintf("%s/%s", jsonDir, jsonName)

	f, err := os.Create(finalLocation)
	check(err)

	return func(data string, close bool) {
		_, err := f.WriteString(data)
		check(err)

		if close {
			f.Close()
		}
	}
}

func getJSONFunc(pretty bool) (func(map[string]string) string, string) {
	var jsonFunc func(map[string]string) string
	var breakLine string
	if pretty {
		breakLine = "\n"
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.MarshalIndent(record, "   ", "   ")
			return "   " + string(jsonData)
		}
	} else {
		breakLine = ""
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.Marshal(record)
			return string(jsonData)
		}
	}

	return jsonFunc, breakLine
}

func writeJSONFile(csvPath string, writerChannel <-chan map[string]string, done chan<- bool, pretty bool) {
	writeString := createStringWriter(csvPath)
	jsonFunc, breakLine := getJSONFunc(pretty)

	fmt.Println("Writing JSON file...")

	writeString("["+breakLine, false)
	first := true
	for {
		record, more := <-writerChannel
		if more {
			if !first {
				writeString(","+breakLine, false)
			} else {
				first = false
			}

			jsonData := jsonFunc(record)
			writeString(jsonData, false)
		} else {
			writeString(breakLine+"]", true)
			fmt.Println("Completed!")
			done <- true
			break
		}
	}
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] <csvFile>\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}

	fileData, err := getFileData()

	if err != nil {
		exitGracefully(err)
	}

	if _, err := checkIfValidFile(fileData.filepath); err != nil {
		exitGracefully(err)
	}

	writerChannel := make(chan map[string]string)
	done := make(chan bool)

	go processCsvFile(fileData, writerChannel)
	go writeJSONFile(fileData.filepath, writerChannel, done, fileData.pretty)

	<-done
}
