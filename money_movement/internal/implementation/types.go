package mm

type wallet struct {
	ID         int32  `json:"id"`
	UserId     string `json:"user_id"`
	walletType string `json:"wallet_type"`
}

type account struct {
	ID          int32  `json:"id"`
	cents       int64  `json:"cents"`
	accountType string `json:"account_type"`
	walletID    int32  `json:"wallet_id"`
}

type transaction struct {
	ID                       int32  `json:"id"`
	pid                      string `json:"pid"`
	srcUserId                string `json:"src_user_id"`
	dstUserId                string `json:"dst_user_id"`
	srcAccountWalletId       int32  `json:"src_account_wallet_id"`
	dstAccountWalletId       int32  `json:"dst_account_wallet_id"`
	srcAccountId             int32  `json:"src_account_id"`
	dstAccountId             int32  `json:"dst_account_id"`
	srcAccountType           string `json:"src_account_type"`
	dstAccountType           string `json:"dst_account_type"`
	finalDstMerchantWalletId int32  `json:"final_dst_merchant_wallet_id"`
	amount                   int64  `json:"amount"`
}
