package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"

	"github.com/niksmo/kafka-connect-practice/internal/consumer"
	"github.com/niksmo/kafka-connect-practice/pkg/logger"
)

func main() {
	config := loadConfig()
	logger := logger.New(config.logLevel)

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM,
	)
	defer cancel()

	consumers := createConsumers(logger, config)

	var wg sync.WaitGroup
	runConsumers(ctx, &wg, consumers)
	logger.Info().Msg("all consumers is running")
	wg.Wait()
	logger.Info().Msg("the application is stopped")
}

type config struct {
	logLevel    string
	brokers     string
	usersGroup  string
	ordersGroup string
	topicUsers  string
	topicOrders string
}

func loadConfig() config {
	return config{
		logLevel:    "info",
		brokers:     "127.0.0.1:19094,127.0.0.1:29094,127.0.0.1:39094",
		usersGroup:  "users-group",
		ordersGroup: "orders-group",
		topicUsers:  "customers.public.users",
		topicOrders: "customers.public.orders",
	}
}

func createConsumers(logger logger.Logger, config config) []consumer.Consumer {
	usersConsumer, err := consumer.New(
		logger, config.brokers, config.topicUsers, config.usersGroup,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create usersConsumer")
	}

	ordersConsumer, err := consumer.New(
		logger, config.brokers, config.topicOrders, config.ordersGroup,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create usersConsumer")
	}

	return []consumer.Consumer{usersConsumer, ordersConsumer}
}

func runConsumers(ctx context.Context,
	wg *sync.WaitGroup, consumers []consumer.Consumer) {
	for _, c := range consumers {
		wg.Add(1)
		go runConsumer(ctx, wg, c)
	}
}

func runConsumer(ctx context.Context,
	wg *sync.WaitGroup, c consumer.Consumer) {
	c.MustRun(ctx)
	defer wg.Done()
	defer c.Close()
}
