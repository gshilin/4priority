// Read all Completed messages from civicrm driver's database and write them to 4priority service

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/MakeNowJust/heredoc"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-querystring/query"
	"github.com/jmoiron/sqlx"
	_ "github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/pkg/errors"

	"ext2fix/pelecard"
	"ext2fix/types"
)

var (
	err error
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

	db, valid, cancel := OpenDb(host, user, password, protocol, dbName)
	defer closeDb(db)

	ReadMessages(db, valid, cancel)
}

// Connect to DB
func OpenDb(host string, user string, password string, protocol string, dbName string) (
	db *sqlx.DB,
	valid *sql.Stmt,
	cancel *sql.Stmt) {

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	if !isTableExists(db, dbName, "bb_ext_requests") {
		log.Fatalf("Table 'bb_ext_requests' does not exist\n")
	}
	if !isTableExists(db, dbName, "bb_ext_pelecard_responses") {
		log.Fatalf("Table 'bb_ext_pelecard_responses' does not exist\n")
	}
	if !isTableExists(db, dbName, "bb_ext_payment_responses") {
		log.Fatalf("Table 'bb_ext_payment_responses' does not exist\n")
	}

	valid, err = db.Prepare("UPDATE bb_ext_requests SET pstatus = 'valid', status = 'valid' WHERE id = ?")
	if err != nil {
		log.Fatalf("Unable to prepare <valid>UPDATE statement: %v\n", err)
	}
	cancel, err = db.Prepare("UPDATE bb_ext_requests SET pstatus = 'cancel', status = 'cancel' WHERE id = ?")
	if err != nil {
		log.Fatalf("Unable to prepare <cancel>UPDATE statement: %v\n", err)
	}

	return
}

func closeDb(db *sqlx.DB) {
	_ = db.Close()
}

func isTableExists(db *sqlx.DB, dbName string, tableName string) (exists bool) {
	var name string

	if err = db.QueryRow(
		"SELECT table_name name FROM information_schema.tables WHERE table_schema = '" + dbName +
			"' AND table_name = '" + tableName + "' LIMIT 1").Scan(&name); err != nil {
		return false
	} else {
		return name == tableName
	}
}

func ReadMessages(db *sqlx.DB, markAsValid *sql.Stmt, markAsCancel *sql.Stmt) {
	card := &pelecard.PeleCard{}
	if err := card.Init(); err != nil {
		log.Fatalf("Init error: %v\n", err)
		return
	}

	type Record struct {
		Id        int    `db:"id"`
		ParamX    string `db:"reference"`
		UserKey   string `db:"user_key"`
		GoodURL   string `db:"good_url"`
		CancelURL string `db:"cancel_url"`
	}
	rec := &Record{}
	totalPaymentsRead := 0
	totalCancelled := 0
	totalValidated := 0
	rows, err := db.Queryx(`
SELECT DISTINCT req.id, req.reference, req.user_key, req.good_url, req.cancel_url
FROM bb_ext_requests req
LEFT OUTER JOIN bb_ext_payment_responses resp ON req.user_key = resp.user_key
WHERE req.status = 'new' AND req.pstatus = 'new'
	AND req.created_at <= DATE_SUB(DATE(NOW()), INTERVAL 5 MINUTE)
	AND resp.user_key IS NULL
	`)
	if err != nil {
		log.Fatalf("Unable to select rows: %v\n", err)
	}

	for rows.Next() {
		// Read messages from DB
		err = rows.StructScan(&rec)
		if err != nil {
			log.Fatalf("Table access error: %v\n", err)
		}
		totalPaymentsRead++
		// check that there is no successful response with the same user_key

		response, err := card.CheckGoodParamX(rec.ParamX)
		if err != nil {
			fmt.Printf("Record ID: %d,\tmarkAsCancel: %v\n", rec.Id, err)
			totalCancelled++
			_, err := markAsCancel.Exec(rec.Id)
			if err != nil {
				fmt.Printf("markAsCancel error: %v\n", err)
			}
			continue
		}
		response.UserKey = rec.UserKey
		if err = UpdateRequest(db, types.PaymentResponse(response)); err != nil {
			log.Fatalf("Record ID: %d,\tParamX: %s\tUpdateRequest error: %v\n", rec.Id, rec.ParamX, err)
			return
		}
		_, err = markAsValid.Exec(rec.Id)
		if err != nil {
			fmt.Printf("markAsValid error: %v\n", err)
			continue
		}
		// connect to GoodURL
		v, _ := query.Values(response)
		confirm(rec.GoodURL, v.Encode(), rec.Id)
		totalValidated++
		fmt.Printf("Record ID: %d (%s) should be marked as Valid\n", rec.Id, rec.ParamX)
	}

	fmt.Printf("Total Records: %d\nTotal Validated: %d\nTotal Cancelled: %d\n", totalPaymentsRead, totalValidated, totalCancelled)
}

func confirm(url string, params string, id int) {
	target := fmt.Sprintf("%s&success=1&%s", url, params)
	body := []byte{}
	resp, err := http.Post(target, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Record ID: %d confirm error: %v response: %#v\n", err, resp)
	}
}

func UpdateRequest(db *sqlx.DB, p types.PaymentResponse) (err error) {
	request := heredoc.Doc(`
		INSERT INTO bb_ext_payment_responses (
			user_key,
			transaction_id, card_hebrew_name, transaction_update_time, credit_card_abroad_card,
			first_payment_total, credit_type, credit_card_brand, voucher_id, station_number,
			additional_details_param_x, credit_card_company_issuer, debit_code, fixed_payment_total,
			credit_card_number, credit_card_exp_date, credit_card_company_clearer, debit_total,
			total_payments, debit_type, transaction_init_time, j_param, transaction_pelecard_id,
			debit_currency
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`)

	err = execInTx(db, request,
		p.UserKey,
		p.TransactionId, p.CardHebrewName, p.TransactionUpdateTime, p.CreditCardAbroadCard,
		p.FirstPaymentTotal, p.CreditType, p.CreditCardBrand, p.VoucherId, p.StationNumber,
		p.AdditionalDetailsParamX, p.CreditCardCompanyIssuer, p.DebitCode, p.FixedPaymentTotal,
		p.CreditCardNumber, p.CreditCardExpDate, p.CreditCardCompanyClearer,
		p.DebitTotal, p.TotalPayments, p.DebitType, p.TransactionInitTime, p.JParam,
		p.TransactionPelecardId, p.DebitCurrency)
	return
}

func execInTx(db *sqlx.DB, query string, args ...interface{}) (err error) {
	var er error
	tx := db.MustBegin()
	_, err = tx.Exec(query, args...)
	if err != nil {
		er = tx.Rollback()
		if er != nil {
			fmt.Println("Query:", query, "\nParams:", args)
			fmt.Println("Query error:", err)
			fmt.Println("Rollback error:", er)
		}
	} else {
		er = tx.Commit()
		if er != nil {
			fmt.Println("Query:", query, "\nParams:", args)
			fmt.Println("Query error:", err)
			fmt.Println("Commit error:", er)
		}
	}
	return
}
