package api

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
)

type AccountBalanceResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  string `json:"result"`
}

// GetAccountBalance Returns the account balance of the address in Matic
func GetAccountBalance(address string) float32 {

	log.Infof("Creating query for getting account balance")

	var httpQuery = fmt.Sprintf("https://api.polygonscan.com/api"+
		"?module=account"+
		"&action=balance"+
		"&address=%s",
		address,
	)
	log.Infof("Query is %s", httpQuery)

	var response = runQuery(httpQuery)
	log.Infof("Response is : %s", string(response))

	// Convert response to struct
	var responseStruct AccountBalanceResponse
	err := json.Unmarshal(response, &responseStruct)
	if err != nil {
		log.Error("Cannot parse response from getAccountBalance endpoint")
	}

	balance, _ := strconv.ParseFloat(responseStruct.Result, 32)
	return float32(balance) / 1e18
}

//runQuery Runs the query for API
func runQuery(query string) []byte {
	apiKey := "824PHEVREU2TFPB3R7T2GR5XXJK1IJBGE2"

	query += fmt.Sprintf("&apikey=%s", apiKey)
	log.Debugf("Query is %s\n", query)

	resp, err := http.Get(query)
	if err != nil {
		println("Got error while getting data")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		println("Error while reading body")
	}
	//bd := string(body)
	return body
}