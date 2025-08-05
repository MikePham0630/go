package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	mm "github.com/MikePham0630/gomicro/internal/implementation"
	pd "github.com/MikePham0630/gomicro/proto"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"google.golang.org/grpc"
)

const (
	dbDriver   = "mysql"
	dbUser     = "money_movement_user"
	dbPassword = "Auth123"
	dbName     = "money_movement"
)

var db *sql.DB

func main() {
	var err error

	// Open a database connection
	dsn := fmt.Sprintf("%s:%s@tcp(mysql-money-movement:3306)/%s", dbUser, dbPassword, dbName)

	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		log.Fatal(err)
	}

	defer func() {
		if db.Close(); err != nil {
			log.Printf("Error closing the database connection: %s", err)
		}
	}()

	// Ping the database to check if the connection is established
	err = db.Ping()
	if err != nil {
		fmt.Println("Error pinging the database:", err)
		log.Fatal(err)
	}

	// grpc server setup
	grpcServer := grpc.NewServer()
	pd.RegisterMoneyMovementServiceServer(grpcServer, mm.NewMoneyMovementImplementation(db))

	// Start the gRPC server
	listener, err := net.Listen("tcp", ":7000")
	if err != nil {
		log.Fatalf("Failed to listen on port 7000: %v", err)
	}
	log.Println("gRPC server is running on port 7000")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

}
