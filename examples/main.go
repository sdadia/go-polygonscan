package main

import (
	log "github.com/sirupsen/logrus"
	"go-polygonscan/api"
)

type T struct {
	Status  string  `json:"status"`
	Status2 float32 `json:"status2"`
	Status3 string  `json:"status3"`
}

func main() {
	//////////////////////////////
	/// Get balance of address ///
	//////////////////////////////
	var address = "0x233BC7d8CBEFdd6e3a531839c553fec5394acc55"
	var balance float64 = api.GetAccountBalance(address)
	log.Infof("Balance is %f matic", balance)

	//////////////////////////////
	/// Get latest matic price ///
	//////////////////////////////
	var maticPrice float32 = api.GetLatestMaticUSDPrice()
	log.Infof("Latest matic price is %f", maticPrice)

	////////////////////////////
	/// Get latest matic gas ///
	////////////////////////////
	var fastGas, proposedGas, safeGas float32 = api.GetMaticGas()
	log.Infof("Matic fees - fast : %f gwei, propossed : %f gwei, safe : %f gwei", fastGas, proposedGas, safeGas)

}
