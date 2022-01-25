# go-polygonscan
Polygon scan API for golang

# Account

* Get account balance - returned value is in matic
```go
var balance float32 = api.GetAccountBalance("02323232c32c323")
```

# Matic Price
* Get the latest price of 1 matic in USD
```go
var maticPrice float32 = api.GetLatestMaticUSDPrice()
```

# Matic Gas
* Get the fast, proposed, safe gas price
```go
var fastGas, proposedGas, safeGas := api.GetMaticGas()
```