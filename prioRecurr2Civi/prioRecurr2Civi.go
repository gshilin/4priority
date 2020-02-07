package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	"github.com/pkg/errors"

	"prioRecurr2Civi/pelecard"
	"prioRecurr2Civi/types"
)

var prioApiUrl = os.Getenv("PRIO_API_URL")
var prioApiOrg = os.Getenv("PRIO_API_ORG")
var prioApiUser = os.Getenv("PRIO_API_USER")
var prioApiPassword = os.Getenv("PRIO_API_PASSWORD")

var civiApiKey = os.Getenv("CIVI_API_KEY")
var civiSiteKey = os.Getenv("CIVI_SITE_KEY")
var civiSiteUrl = os.Getenv("CIVI_SITE_URL")

var civiPaymentProcessor = "75"

type ResponseStruct struct {
	Value []map[string]interface{} `json:"value"`
}

type Contribution struct {
	ID          string  `json:"contribution_id"`
	PayDate     string  `json:"pay_date"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Sku         string  `json:"sku"`
	Description string  `json:"description"`
	Pelecard    types.GetTransDataResponse
}

type CiviGetContribution struct {
	ID                  string `json:"contribution_id"`
	ContactId           string `json:"contact_id"`
	Currency            string `json:"currency"`
	Amount              string `json:"total_amount"`
	ContributionSource  string `json:"contribution_source"`
	FinancialTypeId     string `json:"financial_type_id"`
	ContributionPageId  string `json:"contribution_page_id"`
	PaymentInstrumentId string `json:"payment_instrument_id"`
	CampaignId          string `json:"campaign_id"`
}

type CiviGetContributions struct {
	ID      int `json:"id"`
	IsError int
	Values  map[string]CiviGetContribution `json:"values"`
}

type CiviGetFinancialTypeData struct {
	Name string
}

type CiviGetFinancialType struct {
	Values map[string]CiviGetFinancialTypeData `json:"values"`
}

func main() {
	// TODO make it **YESTERDAY**
	from := "01/01/2018 00:00"
	to := "01/12/2018 00:00"
	RegularTerminal := os.Getenv("PELECARD_TERMINAL")
	log.Println("Regular Terminal")
	handleTerminal(RegularTerminal, from, to)
	RecurrTerminal := os.Getenv("PELECARD_RECURR_TERMINAL")
	log.Println("Recurrent Payments Terminal")
	handleTerminal(RecurrTerminal, from, to)
	log.Println("Done")
}

func handleTerminal(terminal, from, to string) {
	var err error

	// 1. Get list of payments from Pelecard for yesterday and filter out those starting with civicrm
	payments, err := GetListFromPelecard(terminal, from, to)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("Got ", len(payments), " payments")
	if len(payments) == 0 {
		return
	}

	// 2. Get from Priority list of contribution ids
	contributions, err := GetPriorityContributions(payments)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("Got ", len(contributions), " contributions")

	// Get from Civi customer, amount and currency of the above contribution
	// Create new contribution marked as Done
	err = HandleContributions(contributions)
	if err != nil {
		fmt.Println(err)
		return
	}
}

var paymentFromPrio = regexp.MustCompile(`^\d+$`)

func GetListFromPelecard(terminal, from, to string) (payments []types.GetTransDataResponse, err error) {
	card := &pelecard.PeleCard{}
	if err = card.Init(terminal); err != nil {
		return nil, errors.Wrapf(err, "GetListFromPelecard: Unable to initialize: %s", )
	}
	err, response := card.GetTransData(from, to)
	if err != nil {
		return nil, errors.Wrapf(err, "GetTransData: error")
	}
	for _, item := range response {
		if paymentFromPrio.MatchString(item.ParamX) {
			payments = append(payments, item)
		}
	}
	return
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func OpenDb() (db *sqlx.DB) {
	var err error

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

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	return
}

func HandleContributions(contributions []Contribution) (err error) {
	db := OpenDb()
	defer db.Close()

	// Get from Civi customer, amount and currency of the above contribution
	findContactPattern := fmt.Sprintf("%s?entity=Contribution&action=get&api_key=%s&key=%s&json={\"return\":\"payment_instrument_id,campaign_id,financial_type_id,contribution_id,contribution_page_id,contact_id,currency,total_amount,contribution_source,\",\"id\":%%s}", civiSiteUrl, civiApiKey, civiSiteKey)
	findFinancialTypePattern := fmt.Sprintf("%s?entity=FinancialType&action=get&api_key=%s&key=%s&json={\"id\":%%s}", civiSiteUrl, civiApiKey, civiSiteKey)
	createContributionUrl := fmt.Sprintf("%s?entity=Contribution&action=create&api_key=%s&key=%s&json=1", civiSiteUrl, civiApiKey, civiSiteKey)

	insertBbPaymentRespose := `
			INSERT INTO civicrm_bb_payment_responses(trxn_id, cid, cardtype, cardnum, cardexp, firstpay, installments, response, amount, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,  NOW())
		`

	for _, contribution := range contributions {
		fmt.Print("paramX: ", contribution.ID)
		uri := fmt.Sprintf(findContactPattern, contribution.ID)
		resp, err := http.Get(uri)
		if err != nil {
			fmt.Println(" -- Unable to find Contact")
			continue
		}
		payment := CiviGetContributions{}
		_ = json.NewDecoder(resp.Body).Decode(&payment)
		paymentValues := payment.Values[contribution.ID]

		// Find financialTypeId
		uri = fmt.Sprintf(findFinancialTypePattern, paymentValues.FinancialTypeId)
		resp, err = http.Get(uri)
		if err != nil {
			fmt.Printf(" -- Get financial Type error: %v\n", paymentValues.FinancialTypeId)
			continue
		}
		financialType := CiviGetFinancialType{}
		_ = json.NewDecoder(resp.Body).Decode(&financialType)

		financialTypeId := financialType.Values[paymentValues.FinancialTypeId].Name

		// Create new contribution marked as Done
		var formData = url.Values{
			"total_amount":          {paymentValues.Amount},
			"currency":              {paymentValues.Currency},
			"financial_type_id":     {financialTypeId},
			"receive_date":          {contribution.PayDate},
			"contact_id":            {paymentValues.ContactId},
			"contribution_page_id":  {paymentValues.ContributionPageId},
			"source":                {paymentValues.ContributionSource + " (recurrent)"},
			"payment_instrument_id": {paymentValues.PaymentInstrumentId},
			"campaign_id":           {paymentValues.CampaignId},
			"tax_amount":            {"0"},
			"invoice_number":        {"1"},                  // do not send to Priority
			"payment_processor":     {civiPaymentProcessor}, //
			"custom_941":            {"2"},                  // Monthly donation
			"custom_942":            {"1"},                  // Credit Card
		}
		resp, err = http.PostForm(createContributionUrl, formData)
		if err != nil {
			fmt.Printf(" -- Unable create new contribution (%v): %s\n", err, uri)
			continue
		}
		response := map[string]interface{}{}
		_ = json.NewDecoder(resp.Body).Decode(&response)
		if response["is_error"].(float64) == 1 {
			fmt.Printf(" -- Create new contribution %#v error: %s\n", formData, response["error_message"].(string))
			continue
		}

		id := int(response["id"].(float64))
		amount, _ := strconv.ParseFloat(contribution.Pelecard.Amount, 64)
		amount /= 100
		p, err := json.Marshal(contribution.Pelecard)
		if err != nil {
			log.Fatalf("Marshal error: %v\n", err)
		}
		_, err = db.Exec(insertBbPaymentRespose,
			contribution.Pelecard.TrxnId,
			id,
			contribution.Pelecard.CardType,
			contribution.Pelecard.CardNum,
			contribution.Pelecard.CardExp,
			amount,
			contribution.Pelecard.Installments,
			p,
			amount,
		)
		if err != nil {
			log.Fatalf("DB INSERT error: %v\n", err)
		}
		fmt.Printf(" -- Created record id %d\n", id)
	}

	return
}

func GetPriorityContributions(payments []types.GetTransDataResponse) (contributions []Contribution, err error) {
	urlBase := prioApiUrl + prioApiOrg

	for _, payment := range payments {
		fmt.Print(".")
		uri := urlBase + "/PAYMENT2_CHANGES?$filter=PAYMENT eq " + payment.ParamX + "&$select=IVNUM"
		data, err := getPelecardData(uri)
		if err != nil {
			return nil, errors.Wrapf(err, "PAYMENT2_CHANGES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) != 1 {
			log.Printf("############## Payment %s: [1] Data is not array: %#v\n", payment.ParamX, data.Value)
			continue
		}
		ivnum := data.Value[0]["IVNUM"].(string)
		uri = urlBase + "/TINVOICES?$filter=IVNUM eq '" + ivnum + "'&$expand=TPAYMENT2_SUBFORM($select=CCUID,PAYDATE),TFNCITEMS_SUBFORM($select=FNCIREF1)"
		data, err = getPelecardData(uri)
		if err != nil {
			return nil, errors.Wrapf(err, "TINVOICES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) == 0 {
			continue
		}
		value := data.Value[0]
		is46 := ""
		if value["QAMT_PRINT46"] != nil {
			is46 = data.Value[0]["QAMT_PRINT46"].(string)
		}
		if is46 != "D" { // Donation
			//log.Printf("############## Payment %s: is not DONATION, but >%s<\n", item, is46)
			continue
		}
		custname := value["CUSTNAME"].(string)
		ivSubnum := getByRefString(data.Value[0], "TFNCITEMS_SUBFORM", "FNCIREF1")[0]
		results := getByRefString(data.Value[0], "TPAYMENT2_SUBFORM", "CCUID", "PAYDATE")
		//token := results[0][5:15]
		payDate := results[1]
		uri = urlBase + "/CINVOICES?$filter=IVNUM eq '" + ivSubnum + "'&$expand=CINVOICEITEMS_SUBFORM($select=PRICE,ICODE,ACCNAME,DSI_DETAILS)"
		data, err = getPelecardData(uri)
		if err != nil {
			return nil, errors.Wrapf(err, "CINVOICES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) != 1 {
			log.Printf("############## Payment %s: [2] Data is not array: %#v\n", payment.ParamX, data.Value)
			continue
		}
		results = getByRefString(data.Value[0], "CINVOICEITEMS_SUBFORM", "ICODE", "ACCNAME", "DSI_DETAILS")
		currency := results[0]
		sku := results[1]
		description := results[2]
		amount := getByRefInt(data.Value[0], "CINVOICEITEMS_SUBFORM", "PRICE")[0]
		uri = urlBase + "/QAMO_LOADINTENET?$filter=QAMO_CUSTNAME eq '" + custname + "'&$select=QAMT_REFRENCE,QAMO_PRICE,QAMO_CURRNCY,QAMO_PARTNAME"
		data, err = getPelecardData(uri)
		if err != nil {
			return nil, errors.Wrapf(err, "QAMO_LOADINTENET for %s: error\n", payment.ParamX)
		}
		contributionId := ""
		for _, value := range data.Value {
			if value["QAMO_PRICE"].(float64) == float64(amount) &&
				value["QAMO_CURRNCY"].(string) == currency &&
				value["QAMO_PARTNAME"].(string) == sku {
				contributionId = value["QAMT_REFRENCE"].(string)
				break
			}
		}
		if contributionId == "" {
			continue
		}
		if currency == "ש\"ח" {
			currency = "ILS"
		}
		contributions = append(contributions, Contribution{
			ID:          contributionId,
			PayDate:     payDate,
			Amount:      float64(amount),
			Currency:    currency,
			Sku:         sku,
			Description: description,
			Pelecard:    payment,
		})
	}
	fmt.Print("\n")
	return
}

func getPelecardData(uri string) (data ResponseStruct, err error) {
	uri = strings.Replace(uri, " ", "%20", -1)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return data, errors.Wrapf(err, "getPelecardData: Unable to initialize: %s", uri)
	}
	req.Header.Set("Authorization", "Basic "+basicAuth(prioApiUser, prioApiPassword))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return data, errors.Wrapf(err, "getPelecardData: Unable to Get response: %s", uri)
	}
	if resp.StatusCode == 200 { // OK
		_ = json.NewDecoder(resp.Body).Decode(&data)
	} else {
		return data, errors.Wrapf(err, "getPelecardData Server Error: %#v", resp)
	}
	return
}

func getByRefString(ref map[string]interface{}, idx1 string, idx2 ...string) (res []string) {
	jbody, _ := json.Marshal(ref[idx1])
	x := []map[string]string{}
	_ = json.Unmarshal(jbody, &x)
	for _, idx := range idx2 {
		res = append(res, x[0][idx])
	}
	return
}

func getByRefInt(ref map[string]interface{}, idx1 string, idx2 ...string) (res []int) {
	jbody, _ := json.Marshal(ref[idx1])
	x := []map[string]int{}
	_ = json.Unmarshal(jbody, &x)
	for _, idx := range idx2 {
		res = append(res, x[0][idx])
	}
	return
}
