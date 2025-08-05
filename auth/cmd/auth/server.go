package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	auth "github.com/MikePham0630/gomicro/internal/implementation/auth"
	pd "github.com/MikePham0630/gomicro/proto"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"google.golang.org/grpc"
)

const (
	dbDriver = "mysql"
	dbName   = "auth"
)

var db *sql.DB

func main() {
	var err error

	dbUser := "auth_user"
	dbPassword := "Auth123"
	// Opren a database connection
	dsn := fmt.Sprintf("%s:%s@tcp(mysql-auth:3306)/%s", dbUser, dbPassword, dbName)

	//printf("Connecting to database with DSN: %s\n", dsn)
	log.Printf("Connecting to database with DSN: %s\n", dsn)

	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	//grpc server set up
	// grpc server setup
	grpcServer := grpc.NewServer()
	authServiceImplematation := auth.NewAuthImplementation(db)
	pd.RegisterAuthServiceServer(grpcServer, authServiceImplematation)

	// Start the gRPC server
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to listen on port 9000: %v", err)
	}
	log.Println("gRPC server is running on port 9000")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

}
