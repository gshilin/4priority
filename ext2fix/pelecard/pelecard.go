package pelecard

import (
	"bytes"
	"encoding/json"
	"ext2fix/types"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type PeleCard struct {
	Url string `json:"-"`

	User     string `json:"user"`
	Password string `json:"password"`
	Terminal string `json:"terminal"`

	TopText    string `json:",omitempty"`
	BottomText string `json:",omitempty"`
	Language   string `json:",omitempty"`
	LogoUrl    string `json:",omitempty"`

	UserKey   string `json:",omitempty"`
	ParamX    string `json:",omitempty"`
	GoodUrl   string `json:",omitempty"`
	ErrorUrl  string `json:",omitempty"`
	CancelUrl string `json:",omitempty"`

	Total       int `json:",omitempty"`
	Currency    int `json:",omitempty"`
	MinPayments int `json:",omitempty"`
	MaxPayments int `json:",omitempty"`

	ActionType                 string          `json:",omitempty"`
	CardHolderName             string          `json:",omitempty"`
	CustomerIdField            string          `json:",omitempty"`
	Cvv2Field                  string          `json:",omitempty"`
	EmailField                 string          `json:",omitempty"`
	TelField                   string          `json:",omitempty"`
	FeedbackDataTransferMethod string          `json:",omitempty"`
	FirstPayment               string          `json:",omitempty"`
	ShopNo                     int             `json:",omitempty"`
	SetFocus                   string          `json:",omitempty"`
	HiddenPelecardLogo         bool            `json:",omitempty"`
	SupportedCards             map[string]bool `json:",omitempty"`

	TransactionId   string `json:",omitempty"`
	ConfirmationKey string `json:",omitempty"`
	TotalX100       string `json:",omitempty"`
}

func (p *PeleCard) Init() (err error) {
	p.User = os.Getenv("PELECARD_USER")
	p.Password = os.Getenv("PELECARD_PASSWORD")
	p.Terminal = os.Getenv("PELECARD_TERMINAL")
	p.Url = os.Getenv("PELECARD_URL")
	if p.User == "" || p.Password == "" || p.Terminal == "" || p.Url == "" {
		err = fmt.Errorf("PELECARD parameters are missing")
		return
	}

	p.LogoUrl = "https://checkout.kabbalah.info/logo1.png"
	p.MinPayments = 1

	return
}

func (p *PeleCard) GetTransaction(transactionId string) (err error, msg map[string]interface{}) {

	p.TransactionId = transactionId
	if err, msg = p.connect("/GetTransaction"); err != nil {
		return
	}

	return
}

func (p *PeleCard) GetRedirectUrl() (err error, url string) {
	p.ActionType = "J4"
	p.CardHolderName = "hide"
	p.CustomerIdField = "hide"
	p.Cvv2Field = "must"
	p.EmailField = "hide"
	p.TelField = "hide"
	p.FeedbackDataTransferMethod = "POST"
	p.FirstPayment = "auto"
	p.ShopNo = 1000
	p.SetFocus = "CC"
	p.HiddenPelecardLogo = true
	p.SupportedCards = map[string]bool{"Amex": true, "Diners": false, "Isra": true, "Master": true, "Visa": true}

	var result map[string]interface{}
	if err, result = p.connect("/init"); err != nil {
		return
	}
	url = result["URL"].(string)
	return
}

func (p *PeleCard) ValidateByUniqueKey() (valid bool, err error) {
	type validate struct {
		User            string
		Password        string
		Terminal        string
		ConfirmationKey string
		UniqueKey       string
		TotalX100       string
	}

	valid = false
	var v = validate{
		p.User,
		p.Password,
		p.Terminal,
		p.ConfirmationKey,
		p.UserKey,
		p.TotalX100,
	}
	params, _ := json.Marshal(v)
	fmt.Println("https://gateway20.pelecard.biz:443/PaymentGW/ValidateByUniqueKey", "application/json", v)
	resp, err := http.Post("https://gateway20.pelecard.biz:443/PaymentGW/ValidateByUniqueKey", "application/json", bytes.NewBuffer(params))
	if err != nil {
		fmt.Println("Err != nil :(", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Println("StatusCode ", resp.StatusCode)
		return
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)
	if bodyString == "1" {
		valid = true
	}
	return
}

func (p *PeleCard) CheckGoodParamX(paramX string) (response types.CheckResponse, err error) {
	type goodParmX struct {
		User            string `json:"user"`
		Password        string `json:"password"`
		Terminal        string `json:"terminalNumber"`
		ParamX          string `json:"paramX"`
		ShvaSuccessOnly string `json:"shvaSuccessOnly"`
	}

	var v = goodParmX{
		p.User,
		p.Password,
		p.Terminal,
		paramX,
		"true",
	}
	params, _ := json.Marshal(v)
	resp, err := http.Post("https://gateway20.pelecard.biz/services/CheckGoodParamX", "application/json", bytes.NewBuffer(params))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if status, ok := body["StatusCode"]; ok {
		if status == "000" {
			data := body["ResultData"].(map[string]interface{})
			response.TransactionId = data["PelecardTransactionId"].(string)
			response.TransactionUpdateTime = data["CreatedDate"].(string)
			response.CreditCardAbroadCard = data["CreditCardAbroadCard"].(string)
			response.FirstPaymentTotal = data["FirstPaymentTotal"].(string)
			response.CreditType = data["CreditType"].(string)
			response.CreditCardBrand = data["CreditCardCompanyIssuer"].(string)
			response.VoucherId = data["VoucherId"].(string)
			response.StationNumber = data["StationNumber"].(string)
			response.AdditionalDetailsParamX = data["AdditionalDetailsParamX"].(string)
			response.CreditCardCompanyIssuer = data["CreditCardCompanyIssuer"].(string)
			response.DebitCode = data["DebitCode"].(string)
			response.FixedPaymentTotal = data["FixedPaymentTotal"].(string)
			response.CreditCardNumber = data["CreditCardNumber"].(string)
			response.CreditCardExpDate = data["CreditCardExpDate"].(string)
			response.CreditCardCompanyClearer = data["CreditCardCompanyClearer"].(string)
			response.DebitTotal = data["DebitTotal"].(string)
			response.TotalPayments = data["TotalPayments"].(string)
			response.DebitType = data["DebitType"].(string)
			response.TransactionInitTime = data["CreatedDate"].(string)
			response.TransactionPelecardId = response.TransactionId
			response.JParam = data["AdditionalDetailsParamX"].(string)
			response.DebitCurrency = data["DebitCurrency"].(string)
		} else {
			err = fmt.Errorf("%s: %s", status, body["ErrorMessage"])
		}
	}
	return
}

func (p *PeleCard) connect(action string) (err error, result map[string]interface{}) {
	params, _ := json.Marshal(*p)
	resp, err := http.Post(p.Url+action, "application/json", bytes.NewBuffer(params))

	if err != nil {
		return
	}
	defer resp.Body.Close()
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if urlOk, ok := body["URL"]; ok {
		if urlOk.(string) != "" {
			result = make(map[string]interface{})
			result["URL"] = urlOk.(string)
			return
		}
	}
	if msg, ok := body["Error"]; ok {
		msg := msg.(map[string]interface{})
		if errCode, ok := msg["ErrCode"]; ok {
			if errCode.(float64) > 0 {
				err = fmt.Errorf("%d: %s", int(errCode.(float64)), msg["ErrMsg"])
			}
		} else {
			err = fmt.Errorf("0: %s", msg["ErrMsg"])
		}
	} else {
		if status, ok := body["StatusCode"]; ok {
			if status == "000" {
				err = nil
				result = body["ResultData"].(map[string]interface{})
			} else {
				err = fmt.Errorf("%s: %s", status, body["ErrorMessage"])
			}
		}
	}

	return
}
