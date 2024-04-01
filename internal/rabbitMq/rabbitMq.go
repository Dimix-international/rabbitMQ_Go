package rabbitMq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	conn *amqp.Connection //used by client
	ch   *amqp.Channel    //send messages
}

func NewRabbitClient(username, password, host, vhost string) (*RabbitClient, error) {
	connect, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
	if err != nil {
		return nil, err
	}

	channel, err := connect.Channel()
	if err != nil {
		return nil, err
	}

	return &RabbitClient{conn: connect, ch: channel}, nil
}

func (r *RabbitClient) Close() {
	r.ch.Close()
	r.conn.Close()
}
