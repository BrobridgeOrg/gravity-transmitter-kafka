package writer

import (
	"encoding/json"
	"strings"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	database "github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/message_queue"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var DataType_name = map[int32]string{
	0: "boolean",
	1: "binary",
	2: "string",
	3: "uint64",
	4: "int64",
	5: "float64",
	6: "array",
	7: "map",
}

type Field struct {
	Type  string      `json:"type"`
	Field interface{} `json:"field"`
}

type JsonData struct {
	Schema   Schema                 `json:"schema"`
	Playload map[string]interface{} `json:"playload"`
}

type Schema struct {
	Method string  `json:"method"`
	Event  string  `json:"event"`
	Table  string  `json:"table"`
	Type   string  `json:"type"`
	Fields []Field `json:"fields"`
}

type Writer struct {
	connector         *Connector
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
}

func NewWriter() *Writer {
	hostsStr := viper.GetString("kafka.hosts")
	hosts := strings.Split(hostsStr, ",")

	log.WithFields(log.Fields{
		"kafka_server": hosts,
	}).Info("Kafka connect infomation")

	return &Writer{
		connector:         NewConnector(hosts),
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
	}
}

func (writer *Writer) Init() error {

	// Connect to kafka
	err := writer.connector.Connect()
	if err != nil {
		return err
	}

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			writer.completionHandler(database.DBCommand(cmd))
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) ProcessData(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	topic := record.Table

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
		"topic":  topic,
	}).Info("Write record")

	fields := []Field{}
	playload := make(map[string]interface{}, len(record.Fields))
	for _, f := range record.Fields {
		n := Field{}
		n.Field = f.Name
		n.Type = DataType_name[int32(f.Value.Type)]
		fields = append(fields, n)
		playload[f.Name] = gravity_sdk_types_record.GetValue(f.Value)
	}

	schema := Schema{
		Method: gravity_sdk_types_record.Method_name[int32(record.Method)],
		Event:  record.EventName,
		Table:  record.Table,
		Type:   "struct",
		Fields: fields,
	}

	data := JsonData{
		Schema:   schema,
		Playload: playload,
	}

	jsondata, err := json.Marshal(data)
	if err != nil {
		log.Error(err)
	}

	writer.Publish(reference, record, string(jsondata), topic, tables)

	return nil
}

func (writer *Writer) Publish(reference interface{}, record *gravity_sdk_types_record.Record, message string, topic string, tables []string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
ProducerLoop:
	for {
		select {
		case writer.connector.producer.Input() <- msg:
			writer.commands <- &DBCommand{
				Reference: reference,
				Record:    record,
				Tables:    tables,
			}
			return

		case err := <-writer.connector.producer.Errors():
			log.Error("Produced message failure: ", err)
			<-time.After(time.Second * 5)
			log.WithFields(log.Fields{
				"event_name": record.EventName,
				"method":     record.Method.String(),
				"table":      record.Table,
			}).Warn("Retry to write record to database...")

			break ProducerLoop
		}
	}
}
