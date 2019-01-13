// Read all messages from civicrm driver's database and write them to 4priority service

package main

import (
	"github.com/go-resty/resty"
	"time"
	"os"
	"strings"
	_ "fmt"
	"fmt"
)

func main() {
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

	if submit2icount() {
		fmt.Println("Success")
	} else {
		fmt.Println("Failure")
	}
}

func submit2icount() (success bool) {

	params := map[string]string{
		// general
		"compID":           "bneibaruch",
		"user":             "bb",
		"pass":             "an1711",
		"dateissued":       time.Now().Local().Format("2006-01-02"),
		"clientname":       "כללי",
		"client_country":   "IL",
		"docType":          "receipt",
		"desc[]":           "Description",
		"income_type_name": "bb_books",
		"vatid":            "307087395",

		// currency
		"currency": "5",

		// Cash
		// "cash": "1",
		// "cashamount": "0",

		// CC
		"credit":        "1",
		"cc_cardtype[]": "ישראכרט",
		// installments
		//"cc_paymenttype[]":   "2",
		//"ccfirstpayment[]":   "1",
		//"cc_numofpayments[]": "1",
		// single payment
		"cc_paymenttype[]": "1",
		"cctotal[]":        "0",
		"cc_cardnumber[]":  "**********1234",
		"cc_holdername[]":  "Gregory Shilin",

		// Paypal
		// "paypal": "1",
		// "pp_sum": "0",
		// "pp_deal_number": "0",
		// "pp_name_of_payer": "0",
		// "pp_paydate": "0",

		"eft":               "בלינק המצורף תוכל להוריד קבלה על התשלום שביצעת: ",
		"es":                "קבלה לעם - אישור תשלום",
		"hwc":               "חנות ספרים",
		"sendOrig":          "gshilin@gmail.com",
		"lang":              "he",
		"receipt_type_name": "no_group",
		"show_response":     "1",

		// debug
		//"debug": "1",
	}

	resp, err := resty.R().
		SetQueryParams(params).
		Post("https://api.icount.co.il/api/create_doc_auto.php")
	fmt.Printf("Response Body: %v\n", resp.String())

	if err != nil {
		return false
	}

	response := parseResponse(resp.String())

	ok, _ := response["REQUEST_OK"]
	_, link := response["EMAIL_LINK"] // presence
	success = ok == "1" && link
	//fmt.Printf("\nok: %s, link: %t, success: %t\n", ok, link, success)

	return
}

func parseResponse(res string) (h map[string]string) {
	h = make(map[string]string, 10)
	pairs := strings.Split(res, "\n")
	for _, s := range pairs {
		pair := strings.SplitN(s, "=", 2)
		if len(pair) == 2 {
			k := pair[0]
			v := pair[1]
			h[k] = v
			//fmt.Printf("h[%s] = %s\n", k, v)
		}
	}

	return
}
