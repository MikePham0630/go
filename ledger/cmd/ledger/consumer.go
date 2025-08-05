package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/MikePham0630/gomicro/internal/ledger"
)

const (
	dbDriver   = "mysql"
	dbUser     = "root"
	dbPassword = "Admin123"
	dbName     = "ledger"
	topic      = "ledger"
)

var (
	db *sql.DB
	wg sync.WaitGroup
)

type LedgerMsg struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Operation string `json:"operation"`
	Date      string `json:"date"`
}

func main() {
	var err error

	// Open a database connection
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", dbUser, dbPassword, dbName)
	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing the database connection: %s", err)
		}
	}()

	// Ping the database to check if the connection is established
	err = db.Ping()
	if err != nil {
		fmt.Println("Error pinging the database:", err)
		return
	}

	done := make(chan struct{})

	cosumer, err := sarama.NewConsumer([]string{"kafka:9092"}, sarama.NewConfig())
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
	var ledgerMsg LedgerMsg
	if err := json.Unmarshal(msg.Value, &ledgerMsg); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	fmt.Printf("Processing email for Order ID: %s, User ID: %s\n", ledgerMsg.OrderID, ledgerMsg.UserID)

	err := ledger.Insert(db, ledgerMsg.OrderID, ledgerMsg.UserID, ledgerMsg.Amount, ledgerMsg.Operation, ledgerMsg.Date)
	if err != nil {
		log.Printf("Failed to send email: %v", err)
		return
	}
}
