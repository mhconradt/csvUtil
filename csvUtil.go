package main

import (
	"encoding/csv"
	uuid "github.com/nu7hatch/gouuid"
	"io/ioutil"
	"os"
	"strings"
  "fmt"
  "log"
	"regexp"
	"time"
	"bufio"
	"sync"
)

var wg sync.WaitGroup

func main() {
	sourcePath, targetPath := GetFilesToProcess()
	fieldsToFormat := GetFieldsToFormat()
	startTime := time.Now().UnixNano()
	// Get the input file and output file path from command line arguments
	fileContent, err := GetFileContent(sourcePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	rows, err := ReadCSV(fileContent)
  if err != nil {
    fmt.Println(err)
    os.Exit(1)
  }
  filteredRows := FilterEmpty(rows)
	phoneIndexes := GetFieldIndexes(rows[0], fieldsToFormat)
	numRows := len(filteredRows)
	rawRecords := make(chan []string, numRows)
	formattedRecords := make(chan []string, numRows)
	for i := 1; i<numRows; i++ {
		rawRecords <- filteredRows[i]
	}
	close(rawRecords)
	for i := 1; i<numRows; i++ {
		wg.Add(1)
		go FormatRow(rawRecords, formattedRecords, phoneIndexes)
	}
	formattedMatrix := make([][]string, 0)
	firstRowWithUUID := append(rows[0], "uuid")
	formattedMatrix = append(formattedMatrix, firstRowWithUUID)
	wg.Wait()
	// Note to future programmer, close the output channel once the goroutines are done with it
	close(formattedRecords)
	for formattedRow := range formattedRecords {
		formattedMatrix = append(formattedMatrix, formattedRow)
	}
	file, err := os.Create(targetPath)
	if err != nil {
		fmt.Println("Error writing file:", err)
		os.Exit(1)
	}
	writer := csv.NewWriter(file)
	err = WriteRecord(formattedMatrix, writer)
	if err != nil {
		fmt.Println("Error writing to CSV file:", err)
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		log.Fatal(err)
	}
	endTime := time.Now().UnixNano()
	nanosecondsTaken := float64(endTime - startTime)
	billion := 1000000000.0
	secondsTaken := nanosecondsTaken / billion
	msg := fmt.Sprintf("%v records written in %v seconds. Have a nice day!", numRows, secondsTaken)
	fmt.Println(msg)
}

func FormatRow(inputChannel <-chan []string, outputChannel chan<- []string, phoneIndexes []int) {
	for {
		record, ok := <-inputChannel
		if ok {
			phoneFormatted := FormatPhoneNumbers(record, phoneIndexes)
			withUUID := AppendUUID(phoneFormatted)
			outputChannel <- withUUID
		} else {
			wg.Done()
			return
		}
	}
}

func FormatPhoneNumbers(record []string, phoneIndexes []int) []string {
	isPhoneNumber := make(map[int]bool)
	newRecord := make([]string, 0)
	isNumber := regexp.MustCompile(`(?m)([0-9]+)`)
	for _,i := range phoneIndexes {
		isPhoneNumber[i] = true
	}
	for index, item := range record {
		if isPhoneNumber[index] {
			numbersInString := isNumber.FindAllString(item,-1)
			numberString := strings.Join(numbersInString, "")
			newRecord = append(newRecord, numberString)
			continue
		}
		newRecord = append(newRecord, item)
	}
	return newRecord
}

func GetFieldIndexes(headerRow []string, fields []string) []int {
	indexes := make([]int, 0)
	for headerIndex, headerField := range headerRow {
		for _, formatField := range fields {
			if formatField == headerField {
				indexes = append(indexes, headerIndex)
			}
		}
	}
	return indexes
}

func GetFieldsToFormat() []string {
	fmt.Print("Please enter the names of columns with phone numbers to be formatted: ")
	reader := bufio.NewReader(os.Stdin)
	rawResponse, _ := reader.ReadString('\n')
	formattedResponse := TrimString(rawResponse)
	return strings.Split(formattedResponse, " ")
}

func GetFilesToProcess() (string, string) {
	fmt.Print("Please enter the path of the file to process: ")
	reader := bufio.NewReader(os.Stdin)
	rawResponse, _ := reader.ReadString('\n')
	source := TrimString(rawResponse)
	fmt.Print("Please enter the path to write the processed file: ")
	rawResponse, _ = reader.ReadString('\n')
	target := TrimString(rawResponse)
	return source, target
}

func TrimString(s string) string {
	return strings.TrimSuffix(s, "\n")
}

func GetFileContent(fileName string) (string, error) {
	b, err := ioutil.ReadFile(fileName)
  if err != nil {
    fmt.Println(err)
    return "", err
  }
  numBytes := len(b)
  return string(b[:numBytes]), nil
}

func ReadCSV(csvData string) ([][]string, error) {
  var matrix [][]string
	stringReader := strings.NewReader(csvData)
	csvReader := csv.NewReader(stringReader)
	records, err := csvReader.ReadAll()
	if err != nil {
		fmt.Println(err)
    return matrix, err
	}
  return records, nil
}

func FilterEmpty(rows [][]string) [][]string {
  var results [][]string
  for _,v := range rows {
    if len(v) > 0 {
      results = append(results, v)
    }
  }
  return results
}

func AppendUUID(record []string) []string {
	u, err := uuid.NewV4()
	if err != nil {
		fmt.Println(err)
	}
	uuidString := u.String()
	// Modify to work with one only
	return append(record, uuidString)
}

func WriteRecord(input [][]string, writer *csv.Writer) error  {
  for _,record := range input {
		if len(record) > 0 {
			if err := writer.Write(record); err != nil {
				return err
			}
		}
  }
  return nil
}
