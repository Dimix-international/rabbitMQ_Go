package rabbitMq

import (
	"context"
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

// CreateQueue will create a new queue based on given cfgs
func (r *RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
	_, err := r.ch.QueueDeclare(queueName, durable, autodelete, false, false, amqp.Table{})
	return err
}

// CreateBinding will bind the current channel to the given exchange using the routing key provided
func (r *RabbitClient) CreateBinding(queueName string, bindingKey, exchangeName string) error {
	//leaving nowait false - make the channel return error if it is fails
	return r.ch.QueueBind(queueName, bindingKey, exchangeName, false, amqp.Table{})
}

// Send is used to publish payloads into an echange with the given routing key
func (r *RabbitClient) Send(ctx context.Context, exchangeName string, routingKey string, options amqp.Publishing) error {
	//mandatory  - is used to determine if an error should be returned upon failure
	return r.ch.PublishWithContext(ctx, exchangeName, routingKey, true, false, options)
}

func (r *RabbitClient) Close() {
	r.ch.Close()
	r.conn.Close()
}
