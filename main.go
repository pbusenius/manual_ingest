package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/pbusenius/manual_inport/model"
	"github.com/pbusenius/manual_inport/src"
	"github.com/schollz/progressbar/v3"
)

var (
	kafkaUrl   = ""
	kafkaPort  = 0
	schemaUrl  = ""
	schemaPort = 0
	topic      = ""
	hdfsUrl    = ""
	hdfsPort   = 0
	inputFile  = ""
)

func init() {
	flag.StringVar(&kafkaUrl, "kafka-url", "localhost", "Kafka URL")
	flag.IntVar(&kafkaPort, "kafka-port", 19092, "Kafka Port")
	flag.StringVar(&topic, "kafka-topic", "ais-input", "Kafka topic")
	flag.StringVar(&schemaUrl, "schema-url", "localhost", "Kafka Schema URL")
	flag.IntVar(&schemaPort, "schema-port", 18081, "Kafka Schema Port")
	flag.StringVar(&hdfsUrl, "hdfs-url", "localhost", "HDFS URL")
	flag.IntVar(&hdfsPort, "hdfs-port", 8020, "HDFS Port")
	flag.StringVar(&inputFile, "input-file", "", "Input File with HDFS-Paths")
	flag.Parse()

	if len(kafkaUrl) == 0 {
		panic("no Kafka URL defined, please set the -kafka-url flag")
	}

	if len(schemaUrl) == 0 {
		panic("no Kafka Schema URL defined, please set the -schema-url flag")
	}

	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func readInputFile() []string {
	readFile, err := os.Open(inputFile)
	if err != nil {
		fmt.Println(err)
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileLines []string

	for fileScanner.Scan() {
		fileLines = append(fileLines, fileScanner.Text())
	}

	return fileLines
}

func main() {
	producer, err := src.NewKafkaProducer(kafkaUrl, kafkaPort, topic, schemaUrl, schemaPort, model.AisSchema())
	if err != nil {
		log.Panicln("Could not initialize Kafaka Producer")
	}

	csvParser, err := src.NewCsvParser()
	if err != nil {
		log.Panicln("Could not initialize CSV-Parser")
	}

	for _, file := range readInputFile() {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("Could not read File from HDFS: %s", file)
		}

		fmt.Println(file)

		messages, err := csvParser.Parse(data)
		if err != nil {
			log.Fatalf("Could not parse CSV-File: %s", file)
		}

		bar := progressbar.Default(-1)
		for _, message := range messages {
			err = producer.Produce(message)
			if err != nil {
				log.Fatalln("Could not produce message")
			}

			bar.Add(1)
		}

		fmt.Println()
	}

	producer.Flush()

	producer.Close()
}
