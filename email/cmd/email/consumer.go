package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/MikePham0630/gomicro/internal/email"
)

const topic = "email"

var wg sync.WaitGroup

type EmailMsg struct {
	OrderId string `json:"order_id"`
	UserId  string `json:"user_id"`
}

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	done := make(chan struct{})

	cosumer, err := sarama.NewConsumer([]string{"my-cluster-kafka-bootstrap:9092"}, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		close(done)
		if err := cosumer.Close(); err != nil {
			log.Println("Failed to close consumer:", err)
		}
	}()

	partitions, err := cosumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}
	for _, partition := range partitions {
		partitionConsumer, err := cosumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			close(done)
			if err := partitionConsumer.Close(); err != nil {
				log.Println("Failed to close partition consumer:", err)
			}
		}()
		wg.Add(1)
		go awaitMessages(partitionConsumer, partition, done)
	}

	wg.Wait()
}

func awaitMessages(partitionConsumer sarama.PartitionConsumer, partition int32, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message from partition %d: %s\n", partition, string(msg.Value))
			handleMessage(msg)
		case <-done:
			fmt.Printf("received done signal, stopping consumer for partition %d\n", partition)
			return
		}
	}
}

func handleMessage(msg *sarama.ConsumerMessage) {
	var emailMsg EmailMsg
	if err := json.Unmarshal(msg.Value, &emailMsg); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	fmt.Printf("Processing email for Order ID: %s, User ID: %s\n", emailMsg.OrderId, emailMsg.UserId)

	err := email.Send(emailMsg.UserId, emailMsg.OrderId)
	if err != nil {
		log.Printf("Failed to send email: %v", err)
		return
	}
}
