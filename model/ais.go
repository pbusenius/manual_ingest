package model

import (
	"log"

	"github.com/hamba/avro/v2"
)

type AisMessage struct {
	MMSI         string  `csv:"MMSI" avro:"MMSI"`
	IMO          string  `csv:"IMO" avro:"IMO"`
	TIMESTAMPUTC string  `csv:"TIMESTAMPUTC,omitempty" avro:"TIMESTAMPUTC"`
	MESSAGEID    string  `csv:"MESSAGEID,omitempty" avro:"MESSAGEID"`
	DESTINATION  string  `csv:"DESTINATION" avro:"DESTINATION"`
	LONGITUDE    float64 `csv:"LONGITUDE,omitempty" avro:"LONGITUDE"`
	LATITUDE     float64 `csv:"LATITUDE,omitempty" avro:"LATITUDE"`
	SOG          float64 `csv:"SOG,omitempty" avro:"SOG"`
	COG          float64 `csv:"COG,omitempty" avro:"COG"`
	ROT          float64 `csv:"ROT,omitempty" avro:"ROT"`
	SIZEA        int     `csv:"SIZEA,omitempty" avro:"SIZEA"`
	SIZEB        int     `csv:"SIZEB,omitempty" avro:"SIZEB"`
	SIZEC        int     `csv:"SIZEC,omitempty" avro:"SIZEC"`
	SIZED        int     `csv:"SIZED,omitempty" avro:"SIZED"`
}

func AisSchema() avro.Schema {
	schema, err := avro.Parse(`{
		"type": "record",
		"name": "message",
		"namespace": "de.ais.avro",
		"fields" : [
			{"name": "MMSI", "type": "string"},
			{"name": "TIMESTAMPUTC", "type": "string"},
			{"name": "MESSAGEID", "type": "string"},
			{"name": "LONGITUDE", "type": "double"},
			{"name": "LATITUDE", "type": "double"},
			{"name": "SOG", "type": "double"},
			{"name": "COG", "type": "double"}
		]
	}`)
	if err != nil {
		log.Fatalf(err.Error())
	}

	return schema
}

func (message *AisMessage) ToAvro(schema avro.Schema) ([]byte, error) {
	data, err := avro.Marshal(schema, message)
	if err != nil {
		return nil, err
	}

	return data, nil
}
