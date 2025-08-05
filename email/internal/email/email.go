package email

import (
	"fmt"
	"net/smtp"
	"os"
)

func Send(target string, orderID string) error {
	senderEmail := os.Getenv("SENDER_EMAIL")
	password := os.Getenv("EMAIL_PASSWORD")
	recipientEmail := target

	message := []byte("Subject: Order Confirmation\n" +
		"\nYour order has been confirmed.\n" +
		"Order ID: " + orderID + "\n")

	stmpServer := "smtp.gmail.com"
	smtpPort := "587"

	creds := smtp.PlainAuth("", senderEmail, password, stmpServer)

	smtpAddr := fmt.Sprintf("%s:%s", stmpServer, smtpPort)

	err := smtp.SendMail(smtpAddr, creds, senderEmail, []string{recipientEmail}, message)
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}
	fmt.Printf("Email sent to %s for Order ID: %s\n", recipientEmail, orderID)

	return nil
}
