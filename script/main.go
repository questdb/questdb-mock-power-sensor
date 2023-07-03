package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const datasetURL = "https://data.open-power-system-data.org/time_series/latest/time_series_15min_singleindex.csv"

var (
	mqttServer string
	countries  = []string{"DE", "AT", "HU", "NL", "BE", "LU"}
	columns    = []string{
		"utc_timestamp",
		"load_actual_entsoe_transparency",
		"load_forecast_entsoe_transparency",
	}
)

type record struct {
	DateTimeUTC  time.Time `csv:"utc_timestamp"`
	CountryCode  string    `csv:"country"`
	LoadActual   float64   `csv:"load_actual_entsoe_transparency"`
	LoadForecast float64   `csv:"load_forecast_entsoe_transparency"`
}

func init() {
	flag.StringVar(&mqttServer, "mqtt-server", "0.0.0.0:1883", "MQTT server URL")
	flag.Parse()
}

func main() {
	fmt.Println("Downloading dataset...")
	dataset, err := downloadDataset(&http.Client{})
	if err != nil {
		panic(err)
	}

	fmt.Println("Parsing dataset...")
	records, err := parseDataset(dataset)
	if err != nil {
		panic(err)
	}

	fmt.Println("Publishing records...")
	client, err := getMQTTClient(mqttServer)
	if err != nil {
		panic(err)
	}

	for i, r := range records {
		line, err := convertToLineProtocol(r)
		if err != nil {
			panic(err)
		}

		res := client.Publish("sensor", 0, false, line)
		if res.Wait() && res.Error() != nil {
			panic(res.Error())
		}

		fmt.Printf("Published %d/%d records\n", i+1, len(records))
		time.Sleep(1 * time.Second)
	}
}

func convertToLineProtocol(data record) (string, error) {
	var buffer bytes.Buffer

	buffer.WriteString("sensor,")
	buffer.WriteString(fmt.Sprintf("country=%s ", data.CountryCode))
	buffer.WriteString(fmt.Sprintf("load_actual=%f,", data.LoadActual))
	buffer.WriteString(fmt.Sprintf("load_forecast=%f ", data.LoadForecast))
	buffer.WriteString(fmt.Sprintf("%d", time.Now().UnixNano()))

	return buffer.String(), nil
}

// parseDataset parses the dataset and returns a slice of Records. Only those
// records are returned that have a country code combined with the column name
// in the header or the column name is "utc_timestamp". Records are parsed for
// data created after 2018-01-01 00:00:00 UTC.
func parseDataset(dataset []byte) ([]record, error) {
	reader := csv.NewReader(bytes.NewReader(dataset))
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}

	indices := make(map[int]string, len(columns))
	for _, column := range columns {
		for j, col := range header {
			for _, country := range countries {
				if col == "utc_timestamp" || strings.HasPrefix(col, fmt.Sprintf("%s_%s", country, column)) {
					indices[j] = country
					break
				}
			}
		}
	}

	remaining, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading records: %w", err)
	}

	filtered := make([][]string, 0)
	for _, row := range remaining {
		date, err := time.Parse(time.RFC3339, row[0])
		if err != nil {
			return nil, fmt.Errorf("error parsing datetime: %w", err)
		}

		if date.After(time.Date(2017, 12, 31, 23, 59, 0, 0, time.UTC)) {
			filtered = append(filtered, row)
		}
	}

	records := make([]record, 0)
	for _, row := range filtered {
		record := record{}
		for j, value := range row {
			if header[j] == "utc_timestamp" {
				if record.DateTimeUTC, err = time.Parse(time.RFC3339, value); err != nil {
					return nil, fmt.Errorf("error parsing datetime: %w", err)
				}
				if record.DateTimeUTC.Before(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)) {
					continue
				}
			} else if country, ok := indices[j]; ok {
				record.CountryCode = country

				switch column := header[j][len(country)+1:]; column {
				case "load_actual_entsoe_transparency":
					record.LoadActual = mustParseFloat(value)
				case "load_forecast_entsoe_transparency":
					record.LoadForecast = mustParseFloat(value)
				}
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func downloadDataset(client *http.Client) ([]byte, error) {
	resp, err := client.Get(datasetURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading dataset: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("error downloading dataset: status code not OK")
	}

	var dataset bytes.Buffer
	if _, err = dataset.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("error reading dataset: %w", err)
	}

	return dataset.Bytes(), nil
}

func getMQTTClient(broker string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", broker))

	client := mqtt.NewClient(opts)

	if res := client.Connect(); res.Wait() && res.Error() != nil {
		return client, res.Error()
	}

	return client, nil
}

func mustParseFloat(s string) float64 {
	if s == "" {
		return 0
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}
