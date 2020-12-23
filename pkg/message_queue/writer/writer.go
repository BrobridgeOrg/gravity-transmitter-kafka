package writer

import (
	"encoding/binary"
	"encoding/json"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
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

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
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
	connector *Connector
	commands  chan *DBCommand
}

func NewWriter() *Writer {
	hosts := []string{viper.GetString("kafka.hosts")}
	topic_perfix := viper.GetString("kafka.topic_perfix")

	log.WithFields(log.Fields{
		"kafka_server": hosts,
		"topic_perfix": topic_perfix,
	}).Info("Kafka connect infomation")

	return &Writer{
		connector: NewConnector(hosts, topic_perfix),
		commands:  make(chan *DBCommand, 2048),
	}
}

func (writer *Writer) Init() error {

	// Connect to kafka
	err := writer.connector.Connect()
	if err != nil {
		return err
	}

	//TODO: Reconnect

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case _ = <-writer.commands:
			/*
				_, err := writer.db.NamedExec(cmd.QueryStr, cmd.Args)
				if err != nil {
					log.Error(err)
				}
			*/
		}
	}
}

func (writer *Writer) ProcessData(record *transmitter.Record) error {

	topic := writer.connector.topic + record.Table

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
		playload[f.Name] = writer.GetValue(f.Value)
	}

	schema := Schema{
		Method: transmitter.Method_name[int32(record.Method)],
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

	writer.Publish(string(jsondata), topic)

	return nil
}

func (writer *Writer) GetValue(value *transmitter.Value) interface{} {

	switch value.Type {
	case transmitter.DataType_FLOAT64:
		return float64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_INT64:
		return int64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_UINT64:
		return uint64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_BOOLEAN:
		return int8(value.Value[0]) & 1
	case transmitter.DataType_STRING:
		return string(value.Value)
	case transmitter.DataType_MAP:
		mapValue := make(map[string]interface{}, len(value.Map.Fields))
		for _, field := range value.Map.Fields {
			mapValue[field.Name] = writer.GetValue(field.Value)
		}
		return mapValue
	case transmitter.DataType_ARRAY:
		arrayValue := make([]interface{}, len(value.Array.Elements))
		for _, ele := range value.Array.Elements {
			v := writer.GetValue(ele)
			arrayValue = append(arrayValue, v)
		}
		return arrayValue
	}

	// binary
	return value.Value
}

func (writer *Writer) Publish(message string, topic string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	select {
	case writer.connector.producer.Input() <- msg:
	case err := <-writer.connector.producer.Errors():
		log.Error("Produced message failure: ", err)
	}
}
