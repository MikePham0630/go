package mm

import (
	"context"
	"database/sql"
	"errors"

	"github.com/MikePham0630/gomicro/internal/producer"
	pb "github.com/MikePham0630/gomicro/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	insertTransactionQuery = "INSERT INTO transactions (pid, src_user_id, dst_user_id, src_account_wallet_id, dst_account_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	selecTractionQuery     = "SELECT id, pid, src_user_id, dst_user_id, src_account_wallet_id, dst_account_wallet_id, src_account_id, dst_account_id, src_account_type, dst_account_type, final_dst_merchant_wallet_id, amount FROM transactions WHERE pid = ?"
)

type Implementation struct {
	db *sql.DB
	pb.UnimplementedMoneyMovementServiceServer
}

func NewMoneyMovementImplementation(db *sql.DB) *Implementation {
	return &Implementation{
		db: db,
	}
}

func (impl *Implementation) Authorize(ctx context.Context, authorizePayload *pb.AuthorizePayload) (*pb.AuthorizationResponse, error) {
	if authorizePayload.GetCurrency() != "USD" {
		return nil, status.Errorf(codes.InvalidArgument, "only USD currency is supported")
	}

	// Begin a transaction
	tx, err := impl.db.Begin()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	merchantWallet, err := fetchWallet(tx, authorizePayload.MerchantWalletUserId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	custWallet, err := fetchWallet(tx, authorizePayload.CustomerWalletUserId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	srcAccount, err := fetchAccount(tx, custWallet.ID, "DEFAULT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	dstAccount, err := fetchAccount(tx, custWallet.ID, "PAYMENT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	err = transfer(tx, srcAccount, dstAccount, authorizePayload.Cents)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	pid := uuid.NewString()
	err = createTransaction(tx, pid, srcAccount, dstAccount, custWallet, custWallet, merchantWallet, authorizePayload.Cents)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	// End the transaction
	err = tx.Commit()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
	}

	return &pb.AuthorizationResponse{
		Pid: pid,
	}, nil

}

func fetchWallet(tx *sql.Tx, userId string) (wallet, error) {
	var w wallet
	query := "SELECT id, user_id, wallet_type FROM wallets WHERE user_id = ?"
	stmt, err := tx.Prepare(query)
	if err != nil {
		return w, status.Errorf(codes.Internal, "failed to prepare statement: %v", err)
	}
	err = stmt.QueryRow(userId).Scan(&w.ID, &w.UserId, &w.walletType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return w, status.Errorf(codes.NotFound, "wallet not found for user: %s", userId)
		}
		return w, status.Errorf(codes.Internal, "failed to query wallet: %v", err)
	}
	return w, nil
}

func fetchWalletWithWalletId(tx *sql.Tx, walletId int32) (wallet, error) {
	var w wallet
	query := "SELECT id, user_id, wallet_type FROM wallets WHERE id = ?"
	stmt, err := tx.Prepare(query)
	if err != nil {
		return w, status.Errorf(codes.Internal, "failed to prepare statement: %v", err)
	}
	err = stmt.QueryRow(walletId).Scan(&w.ID, &w.UserId, &w.walletType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return w, status.Errorf(codes.NotFound, "wallet not found for ID: %d", walletId)
		}
		return w, status.Errorf(codes.Internal, "failed to query wallet: %v", err)
	}
	return w, nil
}

func fetchAccount(tx *sql.Tx, walletId int32, accountType string) (account, error) {
	var a account
	query := "SELECT id, cents, account_type, wallet_id FROM accounts WHERE wallet_id = ? AND account_type = ?"
	stmt, err := tx.Prepare(query)
	if err != nil {
		return a, status.Errorf(codes.Internal, "failed to prepare statement: %v", err)
	}
	err = stmt.QueryRow(walletId, accountType).Scan(&a.ID, &a.cents, &a.accountType, &a.walletID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return a, status.Errorf(codes.NotFound, "account not found for wallet ID: %d and type: %s", walletId, accountType)
		}
		return a, status.Errorf(codes.Internal, "failed to query account: %v", err)
	}
	return a, nil
}

func transfer(tx *sql.Tx, srcAccount, dstAccount account, amount int64) error {
	if srcAccount.cents < amount {
		return status.Errorf(codes.FailedPrecondition, "insufficient funds in source account")
	}

	// Deduct from source account
	newSrcCents := srcAccount.cents - amount
	stmt, err := tx.Prepare("UPDATE accounts SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update source account: %v", err)
	}

	_, err = stmt.Exec(newSrcCents, srcAccount.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update source account: %v", err)
	}

	// Add to destination account
	newDstCents := dstAccount.cents + amount
	stmt, err = tx.Prepare("UPDATE accounts SET cents = ? WHERE id = ?")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update destination account: %v", err)
	}

	_, err = stmt.Exec(newDstCents, dstAccount.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update source account: %v", err)
	}

	return nil
}

func createTransaction(tx *sql.Tx, pid string, srcAccount, dstAccount account, srcWallet, dstWallet, finalDstWallet wallet, amount int64) error {
	stmt, err := tx.Prepare(insertTransactionQuery)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to prepare insert transaction statement: %v", err)
	}

	_, err = stmt.Exec(pid, srcWallet.UserId, dstWallet.UserId, srcWallet.ID, dstWallet.ID, srcAccount.ID, dstAccount.ID, srcAccount.accountType, dstAccount.accountType, finalDstWallet.ID, amount)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to insert transaction: %v", err)
	}

	return nil
}

func (impl *Implementation) Capture(ctx context.Context, capturePayload *pb.CapturePayload) (*emptypb.Empty, error) {
	//Begin a transaction
	tx, err := impl.db.Begin()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	authorizeTransaction, err := fetchTransaction(tx, capturePayload.Pid)
	if err != nil {
		rollbackErr := tx.Rollback()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}
	srcAccount, err := fetchAccount(tx, authorizeTransaction.dstAccountWalletId, "PAYMENT")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}
	dstMerchantAccount, err := fetchAccount(tx, authorizeTransaction.finalDstMerchantWalletId, "INCOMING")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	err = transfer(tx, srcAccount, dstMerchantAccount, authorizeTransaction.amount)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	merchantWallet, err := fetchWalletWithWalletId(tx, authorizeTransaction.finalDstMerchantWalletId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	customerWallet, err := fetchWallet(tx, authorizeTransaction.srcUserId)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	err = createTransaction(tx, authorizeTransaction.pid, srcAccount, dstMerchantAccount, customerWallet, customerWallet, merchantWallet, authorizeTransaction.amount)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", rollbackErr)
		}
		return nil, err
	}

	//commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
	}

	producer.SendCaptureMessage(authorizeTransaction.pid, authorizeTransaction.srcUserId, authorizeTransaction.amount)

	return &emptypb.Empty{}, nil

}

func fetchTransaction(tx *sql.Tx, pid string) (transaction, error) {
	var t transaction
	stmt, err := tx.Prepare(selecTractionQuery)
	if err != nil {
		return t, status.Errorf(codes.Internal, "failed to prepare statement: %v", err)
	}
	err = stmt.QueryRow(pid).Scan(&t.ID, &t.pid, &t.srcUserId, &t.dstUserId, &t.srcAccountWalletId, &t.dstAccountWalletId, &t.srcAccountId, &t.dstAccountId, &t.srcAccountType, &t.dstAccountType, &t.finalDstMerchantWalletId, &t.amount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, status.Errorf(codes.NotFound, "transaction not found for pid: %s", pid)
		}
		return t, status.Errorf(codes.Internal, "failed to query transaction: %v", err)
	}
	return t, nil
}
