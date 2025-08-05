package ledger

import (
	"database/sql"
	"fmt"
)

func Insert(db *sql.DB, orderID, userID string, amount int64, operation, date string) error {
	query := "INSERT INTO ledger (order_id, user_id, amount, operation, date) VALUES (?, ?, ?, ?, ?)"

	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	_, err = stmt.Exec(query, orderID, userID, amount, operation, date)
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}
	return nil
}
