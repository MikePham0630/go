package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	authpb "github.com/MikePham0630/gomicro/auth"
	mmpb "github.com/MikePham0630/gomicro/money_movement"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var mmClient mmpb.MoneyMovementServiceClient
var authClient authpb.AuthServiceClient

func main() {
	authConn, err := grpc.NewClient("auth:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to auth service: %v", err)
	}
	defer func() {
		if err := authConn.Close(); err != nil {
			log.Printf("Error closing auth connection: %s", err)
		}
	}()

	authClient = authpb.NewAuthServiceClient(authConn)

	mmConn, err := grpc.NewClient("money_movement:7000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to money movement service: %v", err)
	}

	defer func() {
		if err := mmConn.Close(); err != nil {
			log.Printf("Error closing money movement connection: %s", err)
		}
	}()

	mmClient = mmpb.NewMoneyMovementServiceClient(mmConn)

	http.HandleFunc("/login", login)
	http.HandleFunc("/customer/payment/authorize", customerPaymentAuthorize)
	http.HandleFunc("/customer/payment/capture", customerPaymentCapture)

	fmt.Printf("Listening on port 8080")
	errL := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(errL)
	}
}

func login(w http.ResponseWriter, r *http.Request) {
	userName, passowrd, ok := r.BasicAuth()
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ctx := context.Background()
	token, err := authClient.GetToken(ctx, &authpb.Credentials{Username: userName, Password: passowrd})
	if err != nil {
		_, errWrite := w.Write([]byte(err.Error()))
		if errWrite != nil {
			log.Printf("Error writing response: %s", errWrite)
		}
		return
	}
	_, err = w.Write([]byte(token.Jwt))
	if err != nil {
		log.Printf("Error writing response: %s", err)
	}

}

func customerPaymentAuthorize(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	ctx := context.Background()
	_, err := authClient.ValidateToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	type authorizePayload struct {
		CustomerWalletUserId string `json:"customer_wallet_user_id"`
		MerchantWalletUserId string `json:"merchant_wallet_user_id"`
		Cents                int64  `json:"cents"`
		Currency             string `json:"currency"`
	}

	var payload authorizePayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(body, &payload)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	ar, err := mmClient.Authorize(ctx, &mmpb.AuthorizePayload{CustomerWalletUserId: payload.CustomerWalletUserId, MerchantWalletUserId: payload.MerchantWalletUserId, Cents: payload.Cents, Currency: payload.Currency})
	if err != nil {
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Printf("Error writing response: %s", writeErr)
		}
		return
	}

	type response struct {
		Pid string `json:"pid"`
	}

	resp := response{
		Pid: ar.Pid,
	}

	resJSON, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(resJSON)
	if err != nil {
		log.Printf("Error writing response: %s", err)
		return
	}

	//res.Pid
}

func customerPaymentCapture(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	ctx := context.Background()
	_, err := authClient.ValidateToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	type capturePayload struct {
		Pid string `json:"pid"`
	}

	var payload capturePayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(body, &payload)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	_, err = mmClient.Capture(ctx, &mmpb.CapturePayload{Pid: payload.Pid})
	if err != nil {
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Printf("Error writing response: %s", writeErr)
		}
		return
	}

	w.WriteHeader(http.StatusOK)

}
