package main

import (
	"encoding/csv"
	uuid "github.com/nu7hatch/gouuid"
	"io/ioutil"
	"os"
	"strings"
  "fmt"
  "log"
	"time"
)

func main() {
	startTime := time.Now().UnixNano()
	// Get the input file and output file path from command line arguments
	if len(os.Args) < 3 {
		os.Exit(1)
	}
	sourcePath, targetPath := os.Args[1], os.Args[2]
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
	numRows := len(filteredRows)
	withUUID := AppendUUID(filteredRows)
	file, err := os.Create(targetPath)
	if err != nil {
		fmt.Println("Error writing file:", err)
		os.Exit(1)
	}
	writer := csv.NewWriter(file)
	err = WriteRecord(withUUID, writer)
	if err != nil {
		fmt.Println("Error writing to CSV file:", err)
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		log.Fatal(err)
	}
	endTime := time.Now().UnixNano()
	nanosecondsTaken := endTime - startTime
	secondsTaken := nanosecondsTaken / 1000000000
	msg := fmt.Sprintf("%v records written in %v seconds. Have a nice day!", numRows, secondsTaken)
	fmt.Println(msg)
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
		fmt.Println(len(v))
    if len(v) > 0 {
      results = append(results, v)
    }
  }
  return results
}

func AppendUUID(recordMatrix [][]string) [][]string {
	numRows := len(recordMatrix)
	records := make([][]string, numRows)
	for index,record := range recordMatrix {
		if index == 0 {
			record = append(record, "uuid")
			records = append(records, record)
			continue
		}
		u, err := uuid.NewV4()
		if err != nil {
			fmt.Println(err)
		}
		uuidString := u.String()
		record = append(record, uuidString)
		records = append(records, record)
	}
	return records
}

func WriteRecord(input [][]string, writer *csv.Writer) error  {
  for _,record := range input {
		fmt.Println(record)
		fmt.Println(len(record))
		if len(record) > 0 {
			if err := writer.Write(record); err != nil {
				return err
			}
		}
  }
  return nil
}
