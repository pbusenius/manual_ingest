package src

import (
	"encoding/binary"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/pbusenius/manual_inport/model"
	"github.com/riferrei/srclient"
)

type KafkaProducer struct {
	producer         *kafka.Producer
	url              string
	port             int
	topic            string
	schemaUrl        string
	schema           avro.Schema
	schemaId         []byte
	DoneChannel      chan bool
	TerminateChannel chan bool
}

func NewKafkaProducer(url string, port int, toptic string, schemaUrl string, schemaPort int, schema avro.Schema) (*KafkaProducer, error) {
	var k KafkaProducer

	k.url = fmt.Sprintf("%s:%d", url, port)
	k.port = port
	k.topic = toptic
	k.schema = schema
	k.schemaUrl = fmt.Sprintf("http://%s:%d", schemaUrl, schemaPort)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": k.url})
	if err != nil {
		return nil, err
	}

	k.producer = p

	err = k.initSchemaRegistry()
	if err != nil {
		return nil, err
	}

	// start worker function
	go k.startWorker()

	return &k, nil
}

func (p *KafkaProducer) initSchemaRegistry() error {
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(p.schemaUrl)
	kafkaSchema, err := schemaRegistryClient.GetLatestSchema(p.topic)
	if err != nil {
		return err
	}

	if kafkaSchema == nil {
		schemaBytes := []byte(model.AisSchema().String())
		kafkaSchema, err = schemaRegistryClient.CreateSchema(p.topic, string(schemaBytes), srclient.Avro)
		if err != nil {
			return err
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(kafkaSchema.ID()))

	p.schemaId = schemaIDBytes

	return nil
}

func (p *KafkaProducer) startWorker() {
	doTerm := false
	for !doTerm {
		select {
		case e := <-p.producer.Events():
			switch ev := e.(type) {
			case kafka.Error:
				e := ev
				if e.IsFatal() {
					fmt.Printf("FATAL ERROR: %v: terminating\n", e)
				} else {
					fmt.Printf("Error: %v\n", e)
				}
			}

		case <-p.TerminateChannel:
			doTerm = true
		}
	}

	close(p.DoneChannel)
}

func (p *KafkaProducer) Produce(message model.AisMessage) error {
	messageData, err := message.ToAvro(p.schema)
	if err != nil {
		return err
	}

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, p.schemaId...)
	recordValue = append(recordValue, messageData...)

	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(message.MMSI),
		Value:          recordValue,
	}, nil)

	return nil
}

func (p *KafkaProducer) Flush() {
	p.producer.Flush(30 * 1000)
}

func (p *KafkaProducer) Close() {
	p.producer.Close()
}
