package api

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
)

type AccountBalanceResponseStruct struct {
	Status  string  `json:"status"`
	Message string  `json:"message"`
	Result  float64 `json:"result,string"`
}

// GetAccountBalance Returns the account balance of the address in Matic
func GetAccountBalance(address string) (balance float64) {

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
	var responseStruct AccountBalanceResponseStruct
	err := json.Unmarshal(response, &responseStruct)
	if err != nil {
		log.Error("Cannot parse response from getAccountBalance into struct")
	}

	balance = responseStruct.Result / 1e18
	return balance
}

type LatestMaticUSDPriceResponseStruct struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  struct {
		Maticbtc          float32 `json:"maticbtc,string"`
		MaticbtcTimestamp uint32  `json:"maticbtc_timestamp,string"`
		Maticusd          float32 `json:"maticusd,string"`
		MaticusdTimestamp uint32  `json:"maticusd_timestamp,string"`
	} `json:"result"`
}

//GetLatestMaticUSDPrice Returns the latest price of 1 MATIC.
func GetLatestMaticUSDPrice() (price float32) {
	log.Infof("Generating query for getting latest matic price")

	var httpQuery = "https://api.polygonscan.com/api" +
		"?module=stats" +
		"&action=maticprice"
	log.Infof("Query is %s", httpQuery)

	var response = runQuery(httpQuery)
	log.Infof("Response is : %s", string(response))

	var responseStruct LatestMaticUSDPriceResponseStruct
	err := json.Unmarshal(response, &responseStruct)
	if err != nil {
		log.Error("Cannot parse response from getLatestMaticUSDPrice into struct")
	}

	price = responseStruct.Result.Maticusd

	return price

}

type MaticGasResponseStruct struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  struct {
		LastBlock       int32   `json:"LastBlock,string"`
		SafeGasPrice    float32 `json:"safeGasPrice,string"`
		ProposeGasPrice float32 `json:"ProposeGasPrice,string"`
		FastGasPrice    float32 `json:"fastGasPrice,string"`
		UsdPrice        float32 `json:"UsdPrice,string"`
	} `json:"result"`
}

//GetMaticGas Returns the current Safe, Proposed and Fast gas prices - returned values are in GWEI
func GetMaticGas() (fastGas float32, proposedGas float32, safeGas float32) {
	log.Infof("Generating query for getting latest gas fastGas")

	var httpQuery = "https://api.polygonscan.com/api" +
		"?module=gastracker" +
		"&action=gasoracle"
	log.Infof("Query is %s", httpQuery)

	var response = runQuery(httpQuery)
	log.Infof("Response is : %s", string(response))

	var responseStruct MaticGasResponseStruct
	err := json.Unmarshal(response, &responseStruct)
	if err != nil {
		log.Error("Cannot parse response from GetMaticGas into struct. Error is %s", err)
	}

	fastGas = responseStruct.Result.FastGasPrice
	proposedGas = responseStruct.Result.ProposeGasPrice
	safeGas = responseStruct.Result.SafeGasPrice
	return fastGas, proposedGas, safeGas

}

//type getBlockNumberByTimestampResponseStruct struct {
//	Status  string `json:"status"`
//	Message string `json:"message"`
//	Result  int64 `json:"result,string"`
//}
//
//func GetBlockNumberByTimestamp(timestamp time.Time) (blockNumber uint64) {
//	log.Infof("Generating query for getting blocknumber by timestamp %s and %d", timestamp.UTC().String(), timestamp.Unix())
//
//	var httpQuery = fmt.Sprintf("https://api.polygonscan.com/api"+
//		"?module=block"+
//		"&action=getblocknobytime"+
//		"&timestamp=%d", timestamp.UTC().Unix()-500)
//	log.Infof("Query is %s", httpQuery)
//
//	var response = runQuery(httpQuery)
//	log.Infof("Response is : %s", string(response))
//
//	var responseStruct getBlockNumberByTimestampResponseStruct
//	err := json.Unmarshal(response, &responseStruct)
//	if err != nil {
//		log.Error("Cannot parse response for getting block number by timestamp. Error is %s", err)
//	}
//
//	blockNumber = uint64(responseStruct.Result)
//	return blockNumber
//}

//runQuery Runs the query for API
func runQuery(query string) []byte {

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
