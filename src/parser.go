package src

import (
	"bytes"
	"encoding/csv"
	"io"
	"log"

	"github.com/jszwec/csvutil"
	"github.com/pbusenius/manual_inport/model"
)

type CsvParser struct {
	Schema model.AisMessage
}

func NewCsvParser() (*CsvParser, error) {
	return &CsvParser{}, nil
}

func (p *CsvParser) Parse(data []byte) ([]model.AisMessage, error) {
	var aisMessages []model.AisMessage

	r := csv.NewReader(bytes.NewBuffer(data))
	r.Comma = '|'

	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		log.Fatal(err)
	}

	for {
		var message model.AisMessage
		if err := dec.Decode(&message); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		aisMessages = append(aisMessages, message)
	}

	return aisMessages, nil
}
