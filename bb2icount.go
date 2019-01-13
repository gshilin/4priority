// Read all messages from civicrm driver's database and write them to 4priority service

package main

import (
	"log"
	_ "github.com/jmoiron/sqlx"
	_ "github.com/pkg/errors"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/joho/godotenv/autoload"
	"fmt"
	"os"
	"strconv"
	"github.com/jmoiron/sqlx"
	"database/sql"
	"github.com/go-resty/resty"
	"time"
)

// Read messages from database
type Contribution struct {
	ID                string         `db:"ID"`
	Description       string         `db:"JSON"`
	CID               sql.NullString `db:"CID"`
	QAMO_CUSTDES      sql.NullString `db:"QAMO_CUSTDES"`
	QAMO_DETAILS      int64          `db:"QAMO_DETAILS"`
	QAMO_PARTDES      sql.NullString `db:"QAMO_PARTDES"`
	QAMO_PAYMENTCODE  sql.NullString `db:"QAMO_PAYMENTCODE"`
	QAMO_PAYMENTCOUNT sql.NullString `db:"QAMO_PAYMENTCOUNT"`
	QAMO_VALIDMONTH   sql.NullString `db:"QAMO_VALIDMONTH"`
	QAMO_PAYPRICE     float64        `db:"QAMO_PAYPRICE"`
	QAMO_CURRNCY      sql.NullString `db:"QAMO_CURRNCY"`
	QAMO_PAYCODE      int64          `db:"QAMO_PAYCODE"`
	QAMO_FIRSTPAY     float64        `db:"QAMO_FIRSTPAY"`
	QAMO_EMAIL        sql.NullString `db:"QAMO_EMAIL"`
	QAMO_ADRESS       sql.NullString `db:"QAMO_ADRESS"`
	QAMO_CITY         sql.NullString `db:"QAMO_CITY"`
	QAMO_CELL         sql.NullString `db:"QAMO_CELL"`
	QAMO_FROM         sql.NullString `db:"QAMO_FROM"`
	QAMM_UDATE        sql.NullString `db:"QAMM_UDATE"`
	QAMO_LANGUAGE     sql.NullString `db:"QAMO_LANGUAGE"`
}

var err error

func main() {
	// Setup iCount rest interfce
	resty.SetRootCertificate("cacert.pem")
	// Retries are configured per client
	resty.DefaultClient.
	// Set retry count to non zero to enable retries
		SetRetryCount(3).
	// You can override initial retry wait time.
	// Default is 100 milliseconds.
		SetRetryWaitTime(5 * time.Second).
	// MaxWaitTime can be overridden as well.
	// Default is 2 seconds.
		SetRetryMaxWaitTime(20 * time.Second)
	resty.SetDebug(true)
	logFile, _ := os.OpenFile("go-resty.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	resty.SetLogger(logFile)

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
		os.Exit(2)
	}
	password := os.Getenv("CIVI_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	protocol := os.Getenv("CIVI_PROTOCOL")
	if protocol == "" {
		log.Fatalf("Unable to connect without protocol\n")
	}
	startFromS := os.Getenv("CIVI_START_FROM")
	var startFrom int
	if startFromS == "" {
		startFrom = 38800
	} else {
		if startFrom, err = strconv.Atoi(startFromS); err != nil {
			log.Fatalf("Wrong value for Start From: (%s) %s\n", startFromS, err)
		}
	}

	db, stmt := OpenDb(host, user, password, protocol, dbName)
	defer closeDb(db)

	ReadMessages(db, stmt, startFrom)
}

// Connect to DB
func OpenDb(host string, user string, password string, protocol string, dbName string) (db *sqlx.DB, stmt *sql.Stmt) {

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	if !isTableExists(db, dbName, "civicrm_bb_payment_responses") {
		log.Fatalf("Table 'civicrm_bb_payment_responses' does not exist\n")
	}

	stmt, err = db.Prepare("UPDATE civicrm_contribution SET invoice_number = 1 WHERE id = ?")
	if err != nil {
		log.Fatalf("Unable to prepare UPDATE statement: %v\n", err)
	}

	return
}

func closeDb(db *sqlx.DB) {
	db.Close()
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

func ReadMessages(db *sqlx.DB, markAsDone *sql.Stmt, startFrom int) {
	totalPaymentsRead := 0
	totalPaymentsSent := 0
	contribution := Contribution{}
	rows, err := db.Queryx(`
SELECT
  co.id ID,
  ft.description JSON, -- amuta - ORG, card - QAMO_PARTNAME, is46 - QAMO_VAT
  co.id CID, -- to join with BB table
  cc.display_name QAMO_CUSTDES, -- שם לקוח
  (
    SELECT count(1) + 1
    FROM civicrm_participant pa
    WHERE pa.registered_by_id = pp.participant_id
  ) QAMO_DETAILS, -- participants
  SUBSTRING(co.source, 1, 48) QAMO_PARTDES, -- תאור מוצר
  CASE co.payment_instrument_id -- should be select
    WHEN 1 THEN -- Credit Card
      (CASE bb.cardtype
      WHEN 1 THEN 'ISR'
      WHEN 2 THEN 'CAL'
      WHEN 3 THEN 'DIN'
      WHEN 4 THEN 'AME'
      WHEN 6 THEN 'LEU'
      END)
    WHEN 2 THEN -- Cash
      'CAS'
  END QAMO_PAYMENTCODE, -- קוד אמצעי תשלום
  bb.cardnum QAMO_PAYMENTCOUNT, -- מס כרטיס/חשבון
  bb.cardexp QAMO_VALIDMONTH, -- תוקף
  COALESCE(bb.amount, co.total_amount) QAMO_PAYPRICE, -- סכום בפועל
  CASE co.currency
    WHEN 'USD' THEN '$'
    WHEN 'EUR' THEN 'EUR'
    ELSE 'ש""ח'
  END QAMO_CURRNCY, -- קוד מטבע
  bb.installments QAMO_PAYCODE, -- קוד תנאי תשלום
  bb.firstpay QAMO_FIRSTPAY, -- גובה תשלום ראשון
  emails.email QAMO_EMAIL, -- אי מייל
  address.street_address QAMO_ADRESS, -- כתובת
  address.city QAMO_CITY, -- עיר
  -- xxx QAMO_CELL, -- נייד
  country.name QAMO_FROM, -- מקור הגעה (country)
  COALESCE(bb.created_at, co.receive_date) QAMM_UDATE,
  CASE cc.preferred_language WHEN 'he_IL' THEN '01' ELSE '02' END QAMO_LANGUAGE
FROM civicrm_contribution co
  INNER JOIN civicrm_contact cc ON co.contact_id = cc.id
  INNER JOIN civicrm_financial_type ft ON co.financial_type_id = ft.id
  INNER JOIN civicrm_participant_payment pp ON pp.contribution_id = co.id
  LEFT OUTER JOIN civicrm_bb_payment_responses bb ON bb.cid = co.id
  LEFT OUTER JOIN civicrm_address address ON address.contact_id = co.contact_id
  LEFT OUTER JOIN civicrm_country country ON address.country_id = country.id
  LEFT OUTER JOIN civicrm_email emails ON emails.contact_id = co.contact_id
WHERE
  co.id >= ?
  AND co.contribution_status_id = (
    SELECT value contributionStatus
    FROM civicrm_option_value
    WHERE option_group_id = (
      SELECT id contributionStatusID
      FROM civicrm_option_group
      WHERE name = "contribution_status"
      LIMIT 1
    ) AND name = 'Completed' -- only completed payments
    LIMIT 1
  ) AND co.is_test = 0 -- not test payments
  AND co.invoice_number IS NULL -- not submitted yet
	`, startFrom)
	if err != nil {
		log.Fatalf("Unable to select rows: %v\n", err)
	}

	for rows.Next() {
		// Read messages from Sqlite DB
		err = rows.StructScan(&contribution)
		if err != nil {
			log.Fatalf("Table 'civicrm_contribution' access error: %v\n", err)
		}
		// Submit to icount
		if submit2icount(contribution) {

			// Update Reported2prio in case of success
			updateReported2prio(markAsDone, contribution.ID)
			totalPaymentsSent++
		} else {
			log.Printf("Failed to send record with CID=%s to iCount\n", contribution.CID)
		}
		totalPaymentsRead++
	}

	fmt.Printf("Total of %d payments were read from CiviCRM DB\n", totalPaymentsRead)
	fmt.Printf("Total of %d of them were transferred to iCount\n", totalPaymentsSent)
}

func submit2icount(contribution Contribution) (success bool) {
	success = false

	params := map[string]string{
		// general
		"compID": "bneibaruch",
		"user":   "bb",
		"pass":   "an1711",
	}

}

func updateReported2prio(stmt *sql.Stmt, id string) {
	// TODO: perform it
	return

	res, err := stmt.Exec(id)
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	if rowCnt != 1 {
		log.Fatalf("Update error: %d rows were updated instead of 1\n", rowCnt)
	}
}
