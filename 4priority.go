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
	port := os.Getenv("PRIO_PORT")
	if port == "" {
		port = "8080"
	}

	// We handle only one request for now...
	router.HandleFunc("/payment_event", processEvent).Methods("POST")
	router.HandleFunc("/payment_event_shopify", processEventShopify).Methods("POST")

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
	var resp GetTransactionResponse
	err = json.Unmarshal(bodyBytes, &resp)
	if err != nil {
		if err != nil {
			msg := fmt.Sprintf("POST RESPONSE Unmarshal Error %v %s", err, string(bodyBytes))
			logMessage(msg)
			txt := fmt.Sprintf("Unmarshal Error: %v: %s", err, request.Approval)
			notify(w, txt, http.StatusInternalServerError)
			return nil, err
		}
	}

	return &resp, nil
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
	eventProcessing(body, event, w)
}

func processEvent(w http.ResponseWriter, req *http.Request) {
	body, event, err := getEvent(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusOK)
		return
	}
	eventProcessing(body, event, w)
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

func eventProcessing(body []byte, event Event, w http.ResponseWriter) {
	registerRequest(event)
	switch event.Organization {
	case "ben2":
	case "arvut2":
	case "meshp18":
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
	var monthly string
	if event.Token != "" {
		monthly = "Y"
	} else {
		monthly = "N"
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
	var convert = func(str string, flag bool) string {
		return str
	}

	var request = Request{
		UserName:     substr(convert(strings.TrimSpace(event.UserName), true), 0, 48),
		Participants: fmt.Sprintf("%d", event.Participants),
		Income:       strings.TrimSpace(event.Income),
		Description:  substr(convert(strings.TrimSpace(event.Description), true), 0, 120),
		CardType:     event.CardType,
		CardNum:      event.CardNum,
		CardExp:      event.CardExp,
		Amount:       event.Amount,
		Currency:     convert(event.Currency, false),
		Installments: fmt.Sprintf("%02d", event.Installments+7),
		FirstPay:     event.FirstPay,
		Token:        strings.TrimSpace(event.Token),
		Approval:     strings.TrimSpace(event.Approval),
		Is46:         vat,
		Email:        substr(strings.TrimSpace(event.Email), 0, 40),
		Address:      substr(convert(strings.TrimSpace(event.Address), true), 0, 12),
		City:         substr(convert(strings.TrimSpace(event.City), true), 0, 22),
		Country:      substr(convert(strings.TrimSpace(event.Country), true), 0, 12),
		Phone:        substr(convert(strings.TrimSpace(event.Phone), true), 0, 16),
		Language:     event.Language,
		Monthly:      monthly,
		CreatedAt:    createdAt,
		Price:        event.Amount,
		Reference:    event.Reference,
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
	if err != nil {
		// ERROR
		//<FORM
		//  TYPE="QAMO_LOADINTENET">
		//  <InterfaceErrors>
		//    <text>.missing מספר בפלאקארד</text>
		//  </InterfaceErrors>
		//</FORM>
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
			msg := fmt.Sprintf("POST RESPONSE Unmarshal Error %v %s", err, string(bodyBytes))
			logMessage(msg)
			txt := fmt.Sprintf("Unmarshal Error: %v: %s", err, request.Reference)
			notify(w, txt, http.StatusInternalServerError)
			return
		}
		msg := fmt.Sprintf("Error %s", xmlMessage.Error.Message+": "+request.Reference)
		logMessage(msg)
		notify(w, xmlMessage.Error.Message+": "+request.Reference, http.StatusInternalServerError)
		return
	}
	if resp.Error.Message != "" {
		// GENERIC ERROR
		//{
		//	"error":{
		//	"code":"","message":"An error has occurred."
		//  }
		//}
		msg := fmt.Sprintf("Error %s", ": "+request.Reference)
		logMessage(msg)
		notify(w, resp.Error.Message+": "+request.Reference, http.StatusBadRequest)
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

//func reverse(s string) string {
//	chars := []rune(s)
//	for i, j := 0, len(chars)-1; i < j; i, j = i+1, j-1 {
//		chars[i], chars[j] = chars[j], chars[i]
//	}
//	return string(chars)
//}

//func convertDirection4Priority(src string, flag bool) string {
//	if flag {
//		src = strings.Replace(src, "\"", "", -1)
//	}
//	src = strings.Replace(src, "[", "", -1)
//	src = strings.Replace(src, "]", "", -1)
//	src = strings.Replace(src, "'", "", -1)
//	src = strings.Replace(src, "(", "", -1)
//	src = strings.Replace(src, ")", "", -1)
//	if len(src) <= 1 || !strings.Contains("אבגדהוזחטיכלמנסעפצקרשתםןץףך", src[:1]) {
//		return src
//	}
//	var target []string
//	arr := strings.Fields(src)
//	for i := len(arr) - 1; i >= 0; i-- {
//		e := arr[i]
//		r := []rune(e)[0]
//		if strings.ContainsRune("אבגדהוזחטיכלמנסעפצקרשתםןץףך", r) { // Do not convert words without Hebrew chars
//			//if r == '(' || r == ')' {
//			//	e = strings.Replace(e, "(", "左", -1)
//			//	e = strings.Replace(e, ")", "权", -1)
//			//	e = reverse(e)
//			//	e = strings.Replace(e, "权", ")", -1)
//			//	e = strings.Replace(e, "左", "(", -1)
//			//} else {
//			e = reverse(e)
//			//}
//		}
//		target = append(target, e)
//	}
//
//	return strings.Join(target, " ")
//}

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
