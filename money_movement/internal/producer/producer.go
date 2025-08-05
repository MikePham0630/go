package producer

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	emailTopic  = "email"
	ledgerTopic = "ledger"
)

type EmailMsg struct {
	OserId string `json:"order_id"`
	UserId string `json:"user_id"`
}

type LedgerMsg struct {
	OrderId   string `json:"order_id"`
	UserId    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Operation string `json:"operation"` // e.g., "capture", "refund"
	Date      string `json:"date"`      // ISO 8601 format
}

func SendCaptureMessage(pid, userId string, amount int64) {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	// Create sync producer
	producer, err := sarama.NewSyncProducer([]string{"my-cluster-kafka-bootstrap:9092"}, sarama.NewConfig())
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close producer:", err)
		}
	}()

	emailMsg := EmailMsg{
		OserId: pid,
		UserId: userId,
	}

	LedgerMsg := LedgerMsg{
		OrderId:   pid,
		UserId:    userId,
		Amount:    amount,
		Operation: "DEBIT",
		Date:      time.Now().Format("2006-01-02"),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go sendMsg(producer, emailMsg, emailTopic, &wg)
	go sendMsg(producer, LedgerMsg, ledgerTopic, &wg)
	wg.Wait()

	log.Printf("Sending capture message: pid=%s, userId=%s, amount=%d", pid, userId, amount)
	// Example: producer.Publish("capture_topic", message)
}

func sendMsg[T EmailMsg | LedgerMsg](producer sarama.SyncProducer, msg T, topic string, wg *sync.WaitGroup) {
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal message: %v", err)
		return
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(stringMsg),
	}

	// Send the message

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message to topic %s: %v", topic, err)
		return
	}

	log.Printf("Message sent to topic %s at partition %d with offset %d", topic, partition, offset)
	wg.Done()
}
