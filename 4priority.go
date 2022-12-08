// Wait for messages from services and submit them to Priority
// https://prioritysoftware.github.io/restapi

// go build 4priority.go && strip 4priority && upx -9 4priority && cp 4priority /media/sf_projects/bbpriority/

package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	"golang.org/x/net/html"
)

type Event struct {
	ID            string  `json:"id"`
	UserName      string  `json:"name" prio:"QAMO_CUSTDES"`
	Participants  int64   `json:"participants" prio:"QAMO_DETAILS"`
	Income        string  `json:"income" prio:"QAMO_PARTNAME"`
	Description   string  `json:"event" prio:"QAMO_PARTDES"`
	CardType      string  `json:"cardtype" prio:"QAMO_PAYMENTCODE"`
	CardNum       string  `json:"cardnum" prio:"QAMO_PAYMENTCOUNT"`
	CardExp       string  `json:"cardexp" prio:"QAMO_VALIDMONTH"`
	Amount        float64 `json:"amount" prio:"QAMO_PAYPRICE"`
	Amount1       float64 `json:"amount1" prio:"QAMO_PRICE"`
	Currency      string  `json:"currency" prio:"QAMO_CURRNCY"`
	Installments  int64   `json:"installments" prio:"QAMO_PAYCODE"`
	FirstPay      float64 `json:"firstpay" prio:"QAMO_FIRSTPAY"`
	Token         string  `json:"token" prio:"QAMO_CARDNUM"`
	Approval      string  `json:"approval" db:"QAMT_AUTHNUM"`
	Is46          bool    `json:"is46" prio:"QAMO_VAT"`
	Email         string  `json:"email" prio:"QAMO_EMAIL"`
	Address       string  `json:"address" prio:"QAMO_ADRESS"`
	City          string  `json:"city" prio:"QAMO_CITY"`
	Country       string  `json:"country" prio:"QAMO_FROM"`
	Phone         string  `json:"phone" prio:"QAMO_CELL"`
	CreatedAt     string  `json:"created_at" prio:"QAMM_UDATE"`
	Language      string  `json:"language" prio:"QAMO_LANGUAGE"`
	Reference     string  `json:"reference" prio:"QAMT_REFERENCE"`
	Organization  string  `json:"organization"`
	IsVisual      bool    `json:"is_visual"`
	IsUTC         int64   `json:"is_utc,omitempty"`
	TransactionId string  `json:"transaction_id,omitempty"`
	IsRegular     int64   `json:"is_regular,omitempty"`
	OrderId       string  `json:"order_id,omitempty"`
}
type Request struct {
	UserName     string  `json:"QAMO_CUSTDES,omitempty"`
	Participants string  `json:"QAMO_DETAILS,omitempty"`
	Income       string  `json:"QAMO_PARTNAME,omitempty"`
	Description  string  `json:"QAMO_PARTDES,omitempty"`
	CardType     string  `json:"QAMO_PAYMENTCODE,omitempty"`
	CardNum      string  `json:"QAMO_PAYMENTCOUNT,omitempty"`
	CardExp      string  `json:"QAMO_VALIDMONTH,omitempty"`
	Amount       float64 `json:"QAMO_PAYPRICE,omitempty"`
	Currency     string  `json:"QAMO_CURRNCY,omitempty"`
	Installments string  `json:"QAMO_PAYCODE,omitempty"`
	FirstPay     float64 `json:"QAMO_FIRSTPAY,omitempty"`
	Token        string  `json:"QAMO_CARDNUM,omitempty"`
	Approval     string  `json:"QAMT_AUTHNUM,omitempty"`
	Is46         string  `json:"QAMO_VAT,omitempty"`
	Email        string  `json:"QAMO_EMAIL,omitempty"`
	Address      string  `json:"QAMO_ADRESS,omitempty"`
	City         string  `json:"QAMO_CITY,omitempty"`
	Country      string  `json:"QAMO_FROM,omitempty"`
	Phone        string  `json:"QAMO_CELL,omitempty"`
	Language     string  `json:"QAMO_LANGUAGE,omitempty"`
	Monthly      string  `json:"QAMO_MONTHLY,omitempty"`
	CreatedAt    string  `json:"QAMM_UDATE,omitempty"`
	Price        float64 `json:"QAMO_PRICE,omitempty"`
	Reference    string  `json:"QAMT_REFRENCE,omitempty"`
	Reference16  string  `json:"PELCARD16,omitempty"`
}
type GetTransactionRequest struct {
	Organization string `json:"organization"`
	CreatedAt    string `json:"created_at"`
	Approval     string `json:"approval"`
}
type GetTransactionResponse struct {
	ParamX                   string `json:"AdditionalDetailsParamX"`
	BroadcastDate            string `json:"BroadcastDate"`
	BroadcastNo              string `json:"BroadcastNo"`
	CreateDate               string `json:"CreateDate"`
	CreditCardAbroadCard     string `json:"CreditCardAbroadCard"`
	CreditCardCompanyClearer string `json:"CreditCardCompanyClearer"`
	CardType                 string `json:"CreditCardCompanyIssuer"`
	CardNum                  string `json:"CreditCardNumber"`
	CardExp                  string `json:"CreditCardExpDate"`
	DebitApproveNumber       string `json:"DebitApproveNumber"`
	DebitCode                string `json:"DebitCode"`
	DebitCurrency            string `json:"DebitCurrency"`
	Amount                   string `json:"DebitTotal"`
	DebitType                string `json:"DebitType"`
	FirstPay                 string `json:"FirstPaymentTotal"`
	FixedPaymentTotal        string `json:"FixedPaymentTotal"`
	JParam                   string `json:"JParam"`
	TrxnId                   string `json:"PelecardTransactionId"`
	ShvaFileNumber           string `json:"ShvaFileNumber"`
	ShvaResult               string `json:"ShvaResult"`
	Installments             string `json:"TotalPayments"`
	VoucherId                string `json:"VoucherId"`
}
type PriceSet struct {
	ShopMoney struct {
		Amount       string `json:"amount"`
		CurrencyCode string `json:"currency_code"`
	} `json:"shop_money"`
	PresentmentMoney struct {
		Amount       string `json:"amount"`
		CurrencyCode string `json:"currency_code"`
	} `json:"presentment_money"`
}

type Address struct {
	FirstName    *string `json:"first_name"`
	Address1     *string `json:"address1"`
	Phone        *string `json:"phone"`
	City         *string `json:"city"`
	ZIP          *string `json:"zip"`
	Province     *string `json:"province,omitempty"`
	Country      *string `json:"country"`
	LastName     *string `json:"last_name"`
	Address2     *string `json:"address2"`
	Company      *string `json:"company,omitempty"`
	Latitude     *string `json:"latitude,omitempty"`
	Longitude    *string `json:"longitude,omitempty"`
	Name         *string `json:"name"`
	CountryCode  *string `json:"country_code"`
	ProvinceCode *string `json:"province_code,omitempty"`
}
type LineItem struct {
	SKU                 *string `json:"sku"`
	Name                *string `json:"name"`
	Note                *string `json:"note"`
	Email               *string `json:"email"`
	Phone               *string `json:"phone,omitempty"`
	Price               *string `json:"price"`
	Title               *string `json:"title"`
	Number              *int    `json:"number"`
	Quantity            *int    `json:"quantity"`
	TotalPrice          *string `json:"total_price"`
	VariantTitle        *string `json:"variant_title"`
	TotalDiscount       *string `json:"total_discount"`
	BillingAddress      Address `json:"billing_address"`
	TotalDiscounts      *string `json:"total_discounts"`
	ShippingAddress     *string `json:"shipping_address,omitempty"`
	DiscountAllocations []struct {
		Amount                   string   `json:"amount"`
		AmountSet                PriceSet `json:"amount_set"`
		DiscountApplicationIndex int      `json:"discount_application_index"`
	} `json:"discount_allocations"`
}
type Delivery struct {
	City          *string `json:"City"`
	Phone         *string `json:"Phone"`
	ZipCode       *string `json:"ZipCode"`
	Address1      *string `json:"Address 1"`
	Address2      *string `json:"Address 2"`
	OrderNotes    *string `json:"OrderNotes"`
	OrderNumber   *string `json:"OrderNumber"`
	CustomerName  *string `json:"Customer Name"`
	NumberOfItems *string `json:"Number Of Items"`
	LineItems     string  `json:"Line Items"`
}

var (
	prioApiUrl string
	authHeader string
	db         *sqlx.DB
	stmt       *sql.Stmt
	err        error
)

func main() {
	host := os.Getenv("CIVI_HOST")
	if host == "" {
		host = "localhost"
	}
	dbName := os.Getenv("CIVI_DBNAME")
	if dbName == "" {
		dbName = "localhost"
	}
	user := os.Getenv("CIVI_USER")
	if user == "" {
		log.Fatalf("Unable to connect without username\n")
	}
	password := os.Getenv("CIVI_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	protocol := os.Getenv("CIVI_PROTOCOL")
	if protocol == "" {
		log.Fatalf("Unable to connect without protocol\n")
	}
	db, stmt = OpenDb(host, user, password, protocol, dbName)
	defer closeDb(db)

	prioUsername := os.Getenv("PRIO_API_USER")
	if prioUsername == "" {
		log.Fatalf("Unable to connect without username\n")
	}
	prioPassword := os.Getenv("PRIO_API_PASSWORD")
	if prioPassword == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	prioApiUrl = os.Getenv("PRIO_API_URL")
	if prioApiUrl == "" {
		log.Fatalf("Unable to connect without url\n")
	}
	data := prioUsername + ":" + prioPassword
	authHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(data))

	router := mux.NewRouter()

	// We handle only one request for now...
	router.HandleFunc("/payment_event", processEvent).Methods("POST")
	router.HandleFunc("/payment_event_shopify", processEventShopify).Methods("POST")
	router.HandleFunc("/create_delivery_doc", createDeliveryDoc).Methods("POST")

	port := os.Getenv("PRIO_PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Println("SERVING on port", port)
	_ = http.ListenAndServe(":"+port, router)
}

// OpenDb Connect to DB
func OpenDb(host string, user string, password string, protocol string, dbName string) (db *sqlx.DB, stmt *sql.Stmt) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	stmt, err = db.Prepare(
		"INSERT INTO bb_ext_4priority_log (username, participants, income, description, cardtype, cardnum, cardexp, " +
			"amount, currency, installments, firstpay, token, approval, is64, email, address, city, country, phone, created_at, " +
			"language, reference, organization, is_utc) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("Unable to prepare UPDATE statement: %v\n", err)
	}

	return
}

func closeDb(db *sqlx.DB) {
	_ = db.Close()
}

func getTransaction(event Event, body []byte, w http.ResponseWriter) (*GetTransactionResponse, error) {
	var request = GetTransactionRequest{
		Organization: event.Organization,
		CreatedAt:    event.CreatedAt,
		Approval:     event.Approval,
	}
	params, _ := json.Marshal(request)

	client := &http.Client{Timeout: time.Second * 100}
	var url = "https://checkout.kbb1.com/payments/transaction"
	req, err := http.NewRequest("POST", url, strings.NewReader(string(params)))
	if err != nil {
		msg := fmt.Sprintf("POST 1 ERROR %s in %s", err, string(body))
		logMessage(msg)
		notify(w, fmt.Sprintf("%v: %s", err, request.Approval), http.StatusInternalServerError)
		return nil, err
	}
	response, err := client.Do(req)
	if err != nil {
		msg := fmt.Sprintf("POST 2 ERROR %s in %s", err, string(body))
		logMessage(msg)
		notify(w, fmt.Sprintf("%v: %s", err, request.Approval), http.StatusInternalServerError)
		return nil, err
	}
	defer response.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(response.Body)
	if len(bodyBytes) == 0 {
		logMessage("POST " + url)
		msg := fmt.Sprintf("POST RESPONSE (zero length answer)\n\tRequest %#v\nResponse %#v", req, response)
		logMessage(msg)
		msg = fmt.Sprintf("BAD RESPONSE ON %s: %s", request.Approval, response.Status)
		notify(w, msg, response.StatusCode)
		return nil, err
	}
	logMessage(fmt.Sprintf("====> GetTransaction POST RESPONSE ====> %v", string(bodyBytes)))
	errMsg := parseHTML4Error(string(bodyBytes))
	if errMsg != "" {
		msg := fmt.Sprintf("POST RESPONSE HTML Error %s", errMsg)
		logMessage(msg)
		txt := fmt.Sprintf("Unmarshal Error 1: %s: %s", errMsg, request.Approval)
		notify(w, txt, http.StatusInternalServerError)
		return nil, fmt.Errorf(txt)
	}
	var resp GetTransactionResponse
	err = json.Unmarshal(bodyBytes, &resp)
	if err != nil {
		if err != nil {
			msg := fmt.Sprintf("POST RESPONSE Unmarshal Error 1 %v %s", err, string(bodyBytes))
			logMessage(msg)
			txt := fmt.Sprintf("Unmarshal Error 1: %v: %s", err, request.Approval)
			notify(w, txt, http.StatusInternalServerError)
			return nil, err
		}
	}

	return &resp, nil
}

func parseHTML4Error(text string) string {
	msg := fmt.Sprintf("PARSING: %s", text)
	logMessage(msg)

	// ERROR
	//  <html><body>
	// 		<h1 style='color: red;'>Error <code>GetTransactionData unable to find transaction around 2022-08-01 21:39:09 with approval 0082853</code>
	//		</h1>
	//		<br>
	//		<pre></pre>
	//  </body></html>
	var isCode bool

	tkn := html.NewTokenizer(strings.NewReader(text))
	for {
		tt := tkn.Next()
		switch {
		case tt == html.ErrorToken:
			return ""
		case tt == html.StartTagToken:
			t := tkn.Token()
			isCode = t.Data == "code"
		case tt == html.TextToken:
			t := tkn.Token()
			if isCode {
				return t.Data
			}
			isCode = false
		}
	}
}

func createDeliveryDoc(w http.ResponseWriter, req *http.Request) {
	logMessage("-----> createDeliveryDoc")
	lineItems, err := getDelivery(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusOK)
		return
	}
	if len(lineItems) != 1 {
		http.Error(w, fmt.Errorf("lineItems JSON must have exactly one element").Error(), http.StatusOK)
		return
	}
	deliveryDocProcessing(lineItems, w)
}

func processEventShopify(w http.ResponseWriter, req *http.Request) {
	logMessage("-----> processEventShopify")
	body, event, err := getEvent(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusOK)
		return
	}
	// Shopify doesn't have ParamX, let's first fetch it...
	trans, err := getTransaction(event, body, w)
	if err != nil {
		logMessage(fmt.Sprintf("-----> after getTransaction err=%v", err))
		http.Error(w, err.Error(), http.StatusOK)
		return
	}
	event.CardType = "CAL"
	event.CardNum = trans.CardNum
	event.CardExp = trans.CardExp
	event.Reference = trans.ParamX
	eventProcessing(body, event, w, true)
}

func processEvent(w http.ResponseWriter, req *http.Request) {
	body, event, err := getEvent(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusOK)
		return
	}
	eventProcessing(body, event, w, false)
}

func getEvent(w http.ResponseWriter, req *http.Request) (body []byte, event Event, err error) {
	body, err = ioutil.ReadAll(req.Body)
	if err != nil {
		message := fmt.Sprintf("Error reading request body: %v", err)
		logMessage(message)
		notify(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	logMessage(fmt.Sprintf("REQUEST BODY: %s", body))
	defer req.Body.Close()

	if err = json.Unmarshal(body, &event); err != nil {
		message := fmt.Sprintf("Unmarshal error %s body %s", err, string(body))
		logMessage(message)
		fmt.Println(string(body), "\nUnmarshal error:", err)
		notify(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	return
}

func getDelivery(w http.ResponseWriter, req *http.Request) (lineItems []LineItem, err error) {
	var body []byte
	body, err = ioutil.ReadAll(req.Body)
	if err != nil {
		message := fmt.Sprintf("Error reading request body: %v", err)
		logMessage(message)
		notify(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	logMessage(fmt.Sprintf("REQUEST BODY: %s", body))
	defer req.Body.Close()

	var delivery Delivery
	if err = json.Unmarshal(body, &delivery); err != nil {
		message := fmt.Sprintf("Unmarshal error %s body %s", err, body)
		logMessage(message)
		fmt.Println(body, "\nUnmarshal error:", err)
		notify(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	return
}

func deliveryDocProcessing(lineItems []LineItem, w http.ResponseWriter) {
	message := fmt.Sprintf("{\"error\":false,\"message\":\"Handled delivery: %#v\"}", lineItems)

	http.Error(w, message, http.StatusOK)
}

func eventProcessing(body []byte, event Event, w http.ResponseWriter, fill16 bool) {
	registerRequest(event)
	income := strings.TrimSpace(event.Income)
	switch event.Organization {
	case "ben2":
	case "arvut2":
	case "meshp18":
		// take the first part of <income>-GSH-2022
		fields := strings.SplitN(income, "-", 2)
		income = fields[0]
	default:
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("Unknown organization: %s", event.Organization)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), http.StatusInternalServerError)
		msg := fmt.Sprintf("Unknown organization <%s> in %s", event.Organization, string(body))
		logMessage(msg)
		return
	}

	vat := "N"
	if event.Is46 {
		vat = "Y"
	}
	monthly := "N"
	if event.IsRegular == 0 && event.Token != "" {
		monthly = "Y"
	}
	t, err := time.Parse("2006-01-02 15:04:05", event.CreatedAt)
	if err != nil {
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("%v", err)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), http.StatusInternalServerError)
		msg := fmt.Sprintf("Wrong CreatedAt %s in %s", err, string(body))
		logMessage(msg)
		return
	}
	if event.IsUTC == 1 {
		jerusalemTZ, err := time.LoadLocation("Asia/Jerusalem")
		if err != nil {
			msg := fmt.Sprintf("Failed to load location \"Asia/Jerusalem\" %s in %s", err, string(body))
			logMessage(msg)
			log.Fatal(`Failed to load location "Asia/Jerusalem"`)
		}
		t = t.In(jerusalemTZ)
	}
	createdAt := t.Format("02/01/06 15:04")
	if event.Email == "" {
		event.Email = "nomail@kab.co.il"
	}
	var request = Request{
		UserName:     substr(strings.TrimSpace(event.UserName), 0, 48),
		Participants: fmt.Sprintf("%d", event.Participants),
		Income:       income,
		Description:  substr(strings.TrimSpace(event.Description), 0, 120),
		CardType:     event.CardType,
		CardNum:      event.CardNum,
		CardExp:      event.CardExp,
		Amount:       event.Amount,
		Currency:     event.Currency,
		Installments: fmt.Sprintf("%02d", event.Installments+7),
		FirstPay:     event.FirstPay,
		Token:        strings.TrimSpace(event.Token),
		Approval:     strings.TrimSpace(event.Approval),
		Is46:         vat,
		Email:        substr(strings.TrimSpace(event.Email), 0, 40),
		Address:      substr(strings.TrimSpace(event.Address), 0, 12),
		City:         substr(strings.TrimSpace(event.City), 0, 22),
		Country:      substr(strings.TrimSpace(event.Country), 0, 12),
		Phone:        substr(strings.TrimSpace(event.Phone), 0, 16),
		Language:     event.Language,
		Monthly:      monthly,
		CreatedAt:    createdAt,
		Price:        event.Amount,
		Reference:    substr(strings.TrimSpace(event.Reference), 0, 12),
	}
	if fill16 {
		request.Reference16 = event.Reference
		request.Reference = event.OrderId
	}
	params, _ := json.Marshal(request)
	message := fmt.Sprintf("POST: %s", params)
	logMessage(message)

	client := &http.Client{Timeout: time.Second * 100}
	var url = prioApiUrl + event.Organization + "/QAMO_LOADINTENET"
	req, err := http.NewRequest("POST", url, strings.NewReader(string(params)))
	if err != nil {
		msg := fmt.Sprintf("POST 1 ERROR %s in %s", err, string(body))
		logMessage(msg)
		notify(w, fmt.Sprintf("%v: %s", err, request.Reference), http.StatusInternalServerError)
		return
	}
	req.Header.Set("OData-Version", "4.0")
	req.Header.Set("Content-Type", "application/json;odata.metadata=minimal")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", authHeader)
	response, err := client.Do(req)
	if err != nil {
		msg := fmt.Sprintf("POST 2 ERROR %s in %s", err, string(body))
		logMessage(msg)
		notify(w, fmt.Sprintf("%v: %s", err, request.Reference), http.StatusInternalServerError)
		return
	}
	defer response.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(response.Body)
	if len(bodyBytes) == 0 {
		logMessage("POST " + url)
		msg := fmt.Sprintf("POST RESPONSE (zero length answer)\n\tRequest %#v\nResponse %#v", req, response)
		logMessage(msg)
		msg = fmt.Sprintf("BAD RESPONSE ON %s: %s", request.Reference, response.Status)
		notify(w, msg, response.StatusCode)
		return
	}
	msg := fmt.Sprintf("POST RESPONSE %s", string(bodyBytes))
	logMessage(msg)
	var resp struct {
		Error struct {
			Message string
		}
		Line int `json:"QAMO_LINE,omitempty"`
	}
	err = json.Unmarshal(bodyBytes, &resp)
	if err == nil && resp.Error.Message != "" {
		// GENERIC ERROR
		//{
		//	"error":{
		//	"code":"","message":"An error has occurred."
		//  }
		//}
		msg := fmt.Sprintf("Error: %s ref(%s)", resp.Error.Message, request.Reference)
		logMessage(msg)
		notify(w, resp.Error.Message+": "+request.Reference, http.StatusBadRequest)
		return
	}
	if err != nil || resp.Line == 0 {
		// ERROR
		// {"?xml":{"@version":"1.0","@encoding":"utf-8","@standalone":"yes"},"FORM":{"@TYPE":"QAMO_LOADINTENET","InterfaceErrors":{"@XmlFormat":"0","text":"שורה 1- הכנסה לקובץ נכשלה"}}}
		type InterfaceErrors struct {
			XMLName xml.Name `xml:"InterfaceErrors"`
			Message string   `xml:"text"`
		}
		type Form struct {
			XMLName xml.Name        `xml:"FORM"`
			Error   InterfaceErrors `xml:"InterfaceErrors"`
		}
		var xmlMessage Form
		err = xml.Unmarshal(bodyBytes, &xmlMessage)
		if err != nil {
			msg := fmt.Sprintf("POST RESPONSE Unmarshal Error 2 %v %s", err, string(bodyBytes))
			logMessage(msg)
			txt := fmt.Sprintf("Unmarshal Error 2: %v: %s", err, request.Reference)
			notify(w, txt, http.StatusInternalServerError)
			return
		}
		msg := fmt.Sprintf("Error: %s ref(%s)", xmlMessage.Error.Message, request.Reference)
		logMessage(msg)
		notify(w, xmlMessage.Error.Message+": "+request.Reference, http.StatusInternalServerError)
		return
	}

	// SUCCESS
	//{
	//	"@odata.context":"https://pri.kbb1.com/odata/Priority/tabula.ini/ben2/$metadata#QAMO_LOADINTENET/$entity",
	//	"QAMT_REFRENCE":"12345","QAMM_UDATE":"21/01/20 05:19","QAMO_CUSTNAME":null,"QAMO_DATE":null,"QAMO_CUSTDES":"Test Test","QAMO_DETAILS":"1","QAMO_BRANCH":null,"QAMO_AGENT":null,"QAMO_PARTNAME":"40002","QAMO_PARTDES":"\u041e\u043d\u043b\u0430\u0439\u043d-\u0432\u0437\u043d\u043e\u0441: Donate once","QAMO_TQAUNT":0,"QAMO_PRICE":7.00,"QAMO_PAYMENTCODE":"CAL","QAMO_PAYMENTCOUNT":"475787******1111","QAMO_VALIDMONTH":"0621","QAMO_PAYPRICE":5.00,"QAMO_CURRNCY":"EUR","QAMO_PAYCODE":"08","QAMO_FIRSTPAY":5.00,"QAMO_CARDNUM":null,"QAMO_VAT":"Y","QAMO_EMAIL":"test@gmail.com","QAMO_ADRESS":null,"QAMO_CITY":null,"QAMO_CELL":"+375293927607","QAMO_FROM":"Belarus","QAMO_LANGUAGE":"EN","QAMO_MONTHLY":"N","QAMT_IVSTATDES":null,"QAMT_AUTHNUM":null,"QAMM_LOAD":null,"QAMM_ERRFLAG":null,"QAMT_CHECK":null,"QAMM_IVNUM":null,"COUNTER_C":null,
	//	"QAMO_LINE":115520
	//}
	message = fmt.Sprintf("{\"error\":false,\"message\":\"Inserted id: %d, reference: %s\"}", resp.Line, request.Reference)
	logMessage(message)
	http.Error(w, message, http.StatusOK)
}

func logMessage(message string) {
	currentTime := time.Now()
	m := fmt.Sprintf("%s %s", currentTime.Format("2006-01-02 15:04:05"), message)
	fmt.Println(m)
}

func notify(w http.ResponseWriter, message string, code int) {
	msg := map[string]interface{}{"error": true, "message": message}
	m, _ := json.Marshal(msg)
	logMessage(string(m))
	http.Error(w, string(m), code)
}

func substr(s string, pos, length int) (result string) {
	l := pos + length
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if len(result)+size <= l {
			result += string(r)
			s = s[size:]
		} else {
			break
		}
	}
	return
}

func registerRequest(event Event) {
	is64 := 0
	if event.Is46 {
		is64 = 1
	}
	_, _ = stmt.Exec(event.UserName, event.Participants, event.Income, event.Description, event.CardType, event.CardNum, event.CardExp,
		event.Amount, event.Currency, event.Installments, event.FirstPay, event.Token, event.Approval, is64,
		event.Email, event.Address, event.City, event.Country, event.Phone, event.CreatedAt,
		event.Language, event.Reference, event.Organization, event.IsUTC)
}
