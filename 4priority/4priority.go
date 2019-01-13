// Wait for messages from services and write them to database to be used by Priority

package main

import (
	"database/sql"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/denisenkom/go-mssqldb"

	"log"
	"net/http"
	"os"
	"io/ioutil"
	"encoding/json"
	"net/url"
	"time"
	"strings"
	"unicode/utf8"
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
	Amount1      float64 `json:"amount1" prio:"QAMO_PAYMENT"`
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
	Reference    string  `json:"reference" prio:"QAMO_REFERENCE"`
	Organization string  `json:"organization"`
	IsVisual     bool    `json:"is_visual"`
}

var (
	bneiDB, arvutDB, mishDB *sql.DB
)

const numOfUpdates = 20

func main() {
	host := os.Getenv("PRIO_DB_HOST")
	if host == "" {
		log.Fatalf("Unable to connect without host\n")
		os.Exit(2)
	}
	username := os.Getenv("PRIO_DB_USER")
	if username == "" {
		log.Fatalf("Unable to connect without username\n")
		os.Exit(2)
	}
	password := os.Getenv("PRIO_DB_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
		os.Exit(2)
	}

	bneiDB, arvutDB, mishDB = connect2DBs(host, username, password) // side effect: changes bnei and arvut
	defer closeDB(bneiDB, arvutDB, mishDB)

	router := mux.NewRouter()
	port := os.Getenv("PRIO_PORT")
	if port == "" {
		port = "8080"
	}

	// We handle only one request for now...
	router.HandleFunc("/payment_event", processEvent).Methods("POST")

	fmt.Println("SERVING on port", port)
	http.ListenAndServe(":"+port, router)
}

func connect2DBs(host, username, password string) (bneiDB *sql.DB, arvutDB *sql.DB, mishDB *sql.DB) {

	bneiDB = connect2db("ben2", host, username, password)
	arvutDB = connect2db("arvut2", host, username, password)
	mishDB = connect2db("mish", host, username, password)
	return
}

func connect2db(dbname, host, username, password string) (db *sql.DB) {
	var err error

	query := url.Values{}
	query.Add("database", dbname)
	query.Add("log", "63")
	query.Add("connection pooling", "0")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(username, password),
		Host:     host,
		Path:     "PRIORITY_INST",
		RawQuery: query.Encode(),
	}

	if db, err = sql.Open("sqlserver", u.String()); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}

	// really connect to db
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	db.SetMaxOpenConns(numOfUpdates)
	db.SetMaxIdleConns(numOfUpdates)

	return
}

func closeDB(dbs ...*sql.DB) {
	for _, db := range dbs {
		db.Close()
	}
}

func processEvent(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		message := map[string]interface{}{"error": true, "message": "Error reading request body"}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), http.StatusInternalServerError)
		return
	}
	defer req.Body.Close()

	event := Event{}
	if err := json.Unmarshal(body, &event); err != nil {
		fmt.Println(string(body), "\nUnmarshal error:", err)
		return
	}

	// write response to DB
	if err != nil {
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("%v", err)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), 404)
		return
	}

	var db *sql.DB
	switch event.Organization {
	case "ben2":
		db = bneiDB
	case "arvut2":
		db = arvutDB
	case "mish":
		db = mishDB
	default:
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("Unknown organization: %s", event.Organization)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), 404)
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
		http.Error(w, string(m), 404)
		return
	}
	createdAt := t.Format("02/01/06 15:04")
	var convert func(string, bool) string
	if event.IsVisual {
		convert = func(str string, flag bool) string {
			return str
		}
	} else {
		convert = convertDirection4Priority
	}
	_, err = db.Exec(
		`INSERT INTO QAMO_LOADINTENET(
							 QAMO_CUSTDES,
							 QAMO_DETAILS,
							 QAMO_PARTNAME,
							 QAMO_PARTDES,
							 QAMO_PAYMENTCODE,
							 QAMO_PAYMENTCOUNT,
							 QAMO_VALIDMONTH,
							 QAMO_PAYPRICE,
							 QAMO_CURRNCY,
							 QAMO_PAYCODE,
							 QAMO_FIRSTPAY,
							 QAMO_CARDNUM,
							 QAMT_AUTHNUM,
							 QAMO_VAT,
							 QAMO_EMAIL,
							 QAMO_ADRESS,
							 QAMO_CITY,
							 QAMO_FROM,
							 QAMO_CELL,
							 QAMO_LANGUAGE,
							 QAMO_MONTHLY,
							 QAMM_UDATE,
							 QAMO_PRICE,
							 QAMT_REFRENCE
							 )
				 VALUES(@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24)`,
		substr(convert(strings.TrimSpace(event.UserName), true), 0, 48),
		fmt.Sprint(event.Participants),
		strings.TrimSpace(event.Income),
		substr(convert(strings.TrimSpace(event.Description), true), 0, 120),
		event.CardType,
		event.CardNum,
		event.CardExp,
		event.Amount,
		convert(event.Currency, false),
		fmt.Sprintf("%02d", event.Installments+7),
		event.FirstPay,
		strings.TrimSpace(event.Token),
		strings.TrimSpace(event.Approval),
		vat,
		substr(strings.TrimSpace(event.Email), 0, 40),
		substr(convert(strings.TrimSpace(event.Address), true), 0, 12),
		substr(convert(strings.TrimSpace(event.City), true), 0, 22),
		substr(convert(strings.TrimSpace(event.Country), true), 0, 12),
		substr(convert(strings.TrimSpace(event.Phone), true), 0, 16),
		event.Language,
		monthly,
		createdAt,
		event.Amount,
		event.Reference,
	)
	if err != nil {
		message := map[string]interface{}{"error": true, "message": fmt.Sprintf("%v", err)}
		m, _ := json.Marshal(message)
		http.Error(w, string(m), 404)
		return
	}

	lastId := 0
	//err = db.QueryRow("SELECT SCOPE_IDENTITY() FROM QAMO_LOADINTENET;").Scan(&lastId)
	// write response
	message := fmt.Sprintf("{\"error\": false, \"message\": \"Inserted id: %d\"}", lastId)
	http.Error(w, message, 200)
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
	if (flag) {
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
