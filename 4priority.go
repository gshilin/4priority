// Wait for messages from services and submit them to Priority
// https://prioritysoftware.github.io/restapi

package main

import (
	"bytes"
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

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gorilla/mux"
	_ "github.com/joho/godotenv/autoload"
)

type Event struct {
	ID           string  `json:"id"`
	UserName     string  `json:"name" prio:"QAMO_CUSTDES"`
	Participants int64   `json:"participants" prio:"QAMO_DETAILS"`
	Income       string  `json:"income" prio:"QAMO_PARTNAME"`
	Description  string  `json:"event" prio:"QAMO_PARTDES"`
	CardType     string  `json:"cardtype" prio:"QAMO_PAYMENTCODE"`
	CardNum      string  `json:"cardnum" prio:"QAMO_PAYMENTCOUNT"`
	CardExp      string  `json:"cardexp" prio:"QAMO_VALIDMONTH"`
	Amount       float64 `json:"amount" prio:"QAMO_PAYPRICE"`
	Amount1      float64 `json:"amount1" prio:"QAMO_PRICE"`
	Currency     string  `json:"currency" prio:"QAMO_CURRNCY"`
	Installments int64   `json:"installments" prio:"QAMO_PAYCODE"`
	FirstPay     float64 `json:"firstpay" prio:"QAMO_FIRSTPAY"`
	Token        string  `json:"token" prio:"QAMO_CARDNUM"`
	Approval     string  `json:"approval" db:"QAMT_AUTHNUM"`
	Is46         bool    `json:"is46" prio:"QAMO_VAT"`
	Email        string  `json:"email" prio:"QAMO_EMAIL"`
	Address      string  `json:"address" prio:"QAMO_ADRESS"`
	City         string  `json:"city" prio:"QAMO_CITY"`
	Country      string  `json:"country" prio:"QAMO_FROM"`
	Phone        string  `json:"phone" prio:"QAMO_CELL"`
	CreatedAt    string  `json:"created_at" prio:"QAMM_UDATE"`
	Language     string  `json:"language" prio:"QAMO_LANGUAGE"`
	Reference    string  `json:"reference" prio:"QAMT_REFERENCE"`
	Organization string  `json:"organization"`
	IsVisual     bool    `json:"is_visual"`
	IsUTC        int64   `json:"is_utc,omitempty"`
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

var prioApiUrl = os.Getenv("PRIO_API_URL")

func main() {
	username := os.Getenv("PRIO_API_USER")
	if username == "" {
		log.Fatalf("Unable to connect without username\n")
	}
	password := os.Getenv("PRIO_API_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	apiUrl := os.Getenv("PRIO_API_URL")
	if apiUrl == "" {
		log.Fatalf("Unable to connect without url\n")
	}
	prioApiUrl = strings.Replace(apiUrl, "//", "//"+username+":"+password+"@", 1)

	router := mux.NewRouter()
	port := os.Getenv("PRIO_PORT")
	if port == "" {
		port = "8080"
	}

	// We handle only one request for now...
	router.HandleFunc("/payment_event", processEvent).Methods("POST")

	fmt.Println("SERVING on port", port)
	_ = http.ListenAndServe(":"+port, router)
}

func processEvent(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		notify(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer req.Body.Close()

	event := Event{}
	if err := json.Unmarshal(body, &event); err != nil {
		fmt.Println(string(body), "\nUnmarshal error:", err)
		notify(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	switch event.Organization {
	case "ben2":
	case "arvut2":
	case "meshp18":
	default:
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("Unknown organization: %s", event.Organization)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), http.StatusInternalServerError)
		return
	}
	var url = prioApiUrl + "/" + event.Organization + "/QAMO_LOADINTENET"

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
		return
	}
	if event.IsUTC == 1 {
		message := fmt.Sprintf("Non-UTC TZ: before %s", t.Format("02/01/06 15:04"))
		logMessage(message, false)
		jerusalemTZ, err := time.LoadLocation("Asia/Jerusalem")
		if err != nil {
			log.Fatal(`Failed to load location "Local"`)
		}
		t = t.In(jerusalemTZ)
		message = fmt.Sprintf("Non-UTC TZ: after %s", t.Format("02/01/06 15:04"))
		logMessage(message, false)
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
	req, err = http.NewRequest("POST", url, bytes.NewBuffer(params))
	if err != nil {
		notify(w, fmt.Sprintf("%v: %s", err, request.Reference), http.StatusInternalServerError)
		return
	}
	req.Header.Set("OData-Version", "4.0")
	req.Header.Set("Content-Type", "application/json;odata.metadata=minimal")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: time.Second * 100}
	response, err := client.Do(req)
	if err != nil {
		notify(w, fmt.Sprintf("%v: %s", err, request.Reference), http.StatusInternalServerError)
		return
	}
	defer response.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(response.Body)
	type Inner struct {
		Message string `json:"message"`
	}
	var resp struct {
		Error struct {
			Message string
		}
		Line int `json:"QAMO_LINE"`
	}
	if len(bodyBytes) == 0 {
		msg := fmt.Sprintf("This line already exists: %s", request.Reference)
		notify(w, msg, http.StatusInternalServerError)
		return
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
			txt := fmt.Sprintf("Unmarshal Error: %v: %s", err, request.Reference)
			notify(w, txt, http.StatusInternalServerError)
			return
		}
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
		notify(w, resp.Error.Message+": "+request.Reference, http.StatusInternalServerError)
		return
	}

	// SUCCESS
	//{
	//	"@odata.context":"https://pri.kbb1.com/odata/Priority/tabula.ini/ben2/$metadata#QAMO_LOADINTENET/$entity",
	//	"QAMT_REFRENCE":"12345","QAMM_UDATE":"21/01/20 05:19","QAMO_CUSTNAME":null,"QAMO_DATE":null,"QAMO_CUSTDES":"Test Test","QAMO_DETAILS":"1","QAMO_BRANCH":null,"QAMO_AGENT":null,"QAMO_PARTNAME":"40002","QAMO_PARTDES":"\u041e\u043d\u043b\u0430\u0439\u043d-\u0432\u0437\u043d\u043e\u0441: Donate once","QAMO_TQAUNT":0,"QAMO_PRICE":7.00,"QAMO_PAYMENTCODE":"CAL","QAMO_PAYMENTCOUNT":"475787******1111","QAMO_VALIDMONTH":"0621","QAMO_PAYPRICE":5.00,"QAMO_CURRNCY":"EUR","QAMO_PAYCODE":"08","QAMO_FIRSTPAY":5.00,"QAMO_CARDNUM":null,"QAMO_VAT":"Y","QAMO_EMAIL":"test@gmail.com","QAMO_ADRESS":null,"QAMO_CITY":null,"QAMO_CELL":"+375293927607","QAMO_FROM":"Belarus","QAMO_LANGUAGE":"EN","QAMO_MONTHLY":"N","QAMT_IVSTATDES":null,"QAMT_AUTHNUM":null,"QAMM_LOAD":null,"QAMM_ERRFLAG":null,"QAMT_CHECK":null,"QAMM_IVNUM":null,"COUNTER_C":null,
	//	"QAMO_LINE":115520
	//}
	message := fmt.Sprintf("{\"error\":false,\"message\":\"Inserted id: %d, %s\"}", resp.Line, request.Reference)
	logMessage(message, false)
	http.Error(w, message, http.StatusOK)
}

func logMessage(message string, sendmail bool) {
	currentTime := time.Now()
	m := fmt.Sprintf("%s %s", currentTime.Format("2006-01-02 15:04:05"), message)
	fmt.Println(m)
	if sendmail {
		sendEmail(m)
	}
}

func sendEmail(m string) {
	//	serverName := "pro.turbo-smtp.com:587"
	//	//host, _, _ := net.SplitHostPort(serverName)
	//	auth := sasl.NewPlainClient("", "support@kbb1.com", "sE3BM1D2")
	//	from := "4priority@kbb1.com"
	//	to   := []string{"alexmizrachi@gmail.com"}
	//	err := smtp.SendMail(serverName, auth, from, to, []byte(m))
	//	if err != nil {
	//		log.Printf("%s SendMail error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//}
	//return
	//if sendEmail {
	//	serverName := "pro.turbo-smtp.com:25"
	//	//host, _, _ := net.SplitHostPort(serverName)
	//	auth := sasl.NewPlainClient("", "support@kbb1.com", "sE3BM1D2")
	//
	//	//tlsconfig := &tls.Config {
	//	//	ServerName: host,
	//	//}
	//	c, err := smtp.Dial(serverName)
	//	if err != nil {
	//		log.Printf("%s Dial error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//	//if err = c.StartTLS(tlsconfig); err != nil {
	//	//	log.Printf("%s StartTLS error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//	//	return
	//	//}
	//	if err = c.Auth(auth); err != nil {
	//		log.Printf("%s Auth error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//
	//	from := mail.Address{"4priority server", "4priority@kbb1.com"}
	//	to := mail.Address{"Alex Mizrachi", "alexmizrachi@gmail.com"}
	//
	//	headers := make(map[string]string)
	//	headers["From"] = from.String()
	//	headers["To"] = to.String()
	//	headers["Subject"] = "4priority error"
	//	message := ""
	//	for k, v := range headers {
	//		message += fmt.Sprintf("%s: %s\r\n", k, v)
	//	}
	//	message += "\r\n" + m
	//
	//	if err = c.Mail(from.Address); err != nil {
	//		log.Printf("%s Mail error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//	if err = c.Rcpt(to.Address); err != nil {
	//		log.Printf("%s Rcpt error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//	w, err := c.Data()
	//	if err != nil {
	//		log.Printf("%s Data error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//
	//	_, err = w.Write([]byte(message))
	//	if err != nil {
	//		log.Printf("%s Write error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//
	//	err = w.Close()
	//	if err != nil {
	//		log.Printf("%s Close error: %s", currentTime.Format("2006-01-02 15:04:05"), err)
	//		return
	//	}
	//
	//	_ = c.Quit()
}

func notify(w http.ResponseWriter, message string, code int) {
	msg := map[string]interface{}{"error": true, "message": message}
	m, _ := json.Marshal(msg)
	logMessage(string(m), true)
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

func reverse(s string) string {
	chars := []rune(s)
	for i, j := 0, len(chars)-1; i < j; i, j = i+1, j-1 {
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func convertDirection4Priority(src string, flag bool) string {
	if flag {
		src = strings.Replace(src, "\"", "", -1)
	}
	src = strings.Replace(src, "[", "", -1)
	src = strings.Replace(src, "]", "", -1)
	src = strings.Replace(src, "'", "", -1)
	src = strings.Replace(src, "(", "", -1)
	src = strings.Replace(src, ")", "", -1)
	if len(src) <= 1 || !strings.Contains("אבגדהוזחטיכלמנסעפצקרשתםןץףך", src[:1]) {
		return src
	}
	var target []string
	arr := strings.Fields(src)
	for i := len(arr) - 1; i >= 0; i-- {
		e := arr[i]
		r := []rune(e)[0]
		if strings.ContainsRune("אבגדהוזחטיכלמנסעפצקרשתםןץףך", r) { // Do not convert words without Hebrew chars
			//if r == '(' || r == ')' {
			//	e = strings.Replace(e, "(", "左", -1)
			//	e = strings.Replace(e, ")", "权", -1)
			//	e = reverse(e)
			//	e = strings.Replace(e, "权", ")", -1)
			//	e = strings.Replace(e, "左", "(", -1)
			//} else {
			e = reverse(e)
			//}
		}
		target = append(target, e)
	}

	return strings.Join(target, " ")
}
