package consumer

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/niksmo/kafka-connect-practice/pkg/logger"
)

type Consumer struct {
	logger logger.Logger
	kc     *kafka.Consumer
	topic  string
}

func New(log logger.Logger, brokers, topic, group string) (Consumer, error) {
	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     brokers,
		"group.id":              group,
		"heartbeat.interval.ms": 2000,
		"session.timeout.ms":    6000,
		"enable.auto.commit":    true,
		"auto.offset.reset":     "earliest",
	})
	if err != nil {
		return Consumer{}, err
	}
	return Consumer{log, kc, topic}, nil
}

func (c Consumer) MustRun(ctx context.Context) {
	if err := c.kc.Subscribe(c.topic, nil); err != nil {
		c.logger.Fatal().Err(err).Send()
	}

	readStream := make(chan any)

	go c.readMessages(readStream)

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-readStream:
			c.handleMsg(m)
		}
	}
}

func (c Consumer) Close() {
	if err := c.kc.Close(); err != nil {
		c.logger.Error().Err(err).Send()
		return
	}
	c.logger.Info().Str("consumerTopic", c.topic).Msg("close successfull")
}

func (c Consumer) readMessages(ch chan<- any) {
	c.logger.Info().Str("topic", c.topic).Msg("waiting messages")
	for {
		msg, err := c.kc.ReadMessage(-1)
		if err != nil {
			ch <- err
			continue
		}
		ch <- msg
	}
}

func (c Consumer) handleMsg(msg any) {
	switch m := msg.(type) {
	case *kafka.Message:
		fmt.Println(string(m.Value))
	case error:
		c.logger.Error().Err(m).Msg("receive erorr")
	default:
		c.logger.Warn().Any("unexpected", m).Msg("receive unexpected type")
	}
}
