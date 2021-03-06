package writer

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type Connector struct {
	producer sarama.AsyncProducer
	hosts    []string
}

func NewConnector(hosts []string) *Connector {
	return &Connector{
		hosts: hosts,
	}
}

func (connector *Connector) Connect() error {

	log.WithFields(log.Fields{
		"hosts": connector.hosts,
	}).Info("Connecting to Kafka server")

	config := sarama.NewConfig()

	prd, err := sarama.NewAsyncProducer(connector.hosts, config)
	if err != nil {
		return err
	}

	connector.producer = prd

	return nil
}
