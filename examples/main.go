package main

import (
	log "github.com/sirupsen/logrus"
	"go-polygonscan/api"
)

func main() {
	//////////////////////////////
	/// Get balance of address ///
	//////////////////////////////
	var address = "0x233BC7d8CBEFdd6e3a531839c553fec5394acc55"
	var balance float32 = api.GetAccountBalance(address)
	log.Infof("Balance is %f matic", balance)

	//////////////////////////////
	/// Get latest matic price ///
	//////////////////////////////
	var maticPrice float32 = api.GetLatestMaticUSDPrice()
	log.Infof("Latest matic price is %f", maticPrice)
}
