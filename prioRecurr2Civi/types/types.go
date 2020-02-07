package types

type GetTransDataResponse struct {
	Amount                   string `json:"DebitTotal"`
	DebitCurrency            string `json:"DebitCurrency"`
	ParamX                   string `json:"AdditionalDetailsParamX"`
	TrxnId                   string `json:"PelecardTransactionId"`
	CardType                 string `json:"CreditCardCompanyIssuer"`
	CardNum                  string `json:"CreditCardNumber"`
	CardExp                  string `json:"CreditCardExpDate"`
	FirstPay                 string `json:"FirstPaymentTotal"`
	Installments             string `json:"TotalPayments"`
	CreateDate               string `json:"CreateDate"`
	BroadcastDate            string `json:"BroadcastDate"`
	BroadcastNo              string `json:"BroadcastNo"`
	VoucherId                string `json:"VoucherId"`
	ShvaResult               string `json:"ShvaResult"`
	ShvaFileNumber           string `json:"ShvaFileNumber"`
	CreditCardCompanyClearer string `json:"CreditCardCompanyClearer"`
	CreditCardAbroadCard     string `json:"CreditCardAbroadCard"`
	DebitType                string `json:"DebitType"`
	DebitCode                string `json:"DebitCode"`
	DebitApproveNumber       string `json:"DebitApproveNumber"`
	FixedPaymentTotal        string `json:"FixedPaymentTotal"`
}
