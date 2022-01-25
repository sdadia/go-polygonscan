package main

import (
	log "github.com/sirupsen/logrus"
	"go-polygonscan/api"
)

func main() {

	var address = "0x233BC7d8CBEFdd6e3a531839c553fec5394acc55"

	var balance = api.GetAccountBalance(address)

	log.Infof("Balance is %f matic", balance)

}
