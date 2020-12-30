package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ava-labs/avalanchego/api"
	"github.com/ava-labs/avalanchego/api/keystore"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/sirupsen/logrus"
)

var (
	uri                    = "http://127.0.0.1:9650"
	userPass               = api.UserPass{Username: "aaronbuchwald", Password: "iu3br37h4973h41972h47912"}
	privateKey             = "PrivateKey-ewoqjP7PxY4yr3iLTpLisriqt94hdyDFNgchSxGGztUrTXtNN"
	minimumDelay           = 2 * time.Second
	varSleep               = 8 * time.Second
	avaxAssetID            = "AVAX"
	txFee           uint64 = 1000000
	maxSendDuration        = 24 * time.Hour
	requestTimeout         = 5 * time.Second
	numAddresses           = 10
	maxBurstSize           = 1000
)

func getAddresses(keystore *keystore.Client, xChainClient *avm.Client) ([]string, uint64, error) {
	keystore.CreateUser(userPass)

	originalAddress, err := xChainClient.ImportKey(userPass, privateKey)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to import key: %s", err)
	}
	logrus.Infof("Imported key with address: %s", originalAddress)

	addresses, err := xChainClient.ListAddresses(userPass)
	if err != nil {
		return nil, 0, fmt.Errorf("Couldn't list addresses: %s", err)
	}
	logrus.Infof("Starting with %d addresses", len(addresses))
	balance := uint64(0)
	for _, addr := range addresses {
		balanceReply, err := xChainClient.GetBalance(addr, "AVAX")
		if err != nil {
			panic(err)
		}
		balance += uint64(balanceReply.Balance)
	}
	if len(addresses) > numAddresses {
		addresses = addresses[:numAddresses]
	}
	logrus.Infof("Found AVAX balance of %d", balance)

	for len(addresses) < numAddresses {
		addr, err := xChainClient.CreateAddress(userPass)
		logrus.Infof("Created address: %s", addr)
		if err != nil {
			return nil, 0, fmt.Errorf("Failed to create new address: %s", err)
		}
		addresses = append(addresses, addr)
	}

	return addresses, balance, nil
}

func confirmTxs(xChainClient *avm.Client, txIDs []ids.ID) error {
	for _, txID := range txIDs {
		status, err := xChainClient.ConfirmTx(txID, 10, time.Second)
		if err != nil {
			return fmt.Errorf("Failed to confirm tx due to %s", err)
		}
		if status != choices.Accepted {
			return fmt.Errorf("transaction %s was not accepted after 10 seconds had status %s", txID, status)
		}
		logrus.Infof("Confirmed %s", txID)
	}

	return nil
}

func sendRandomTx(xChainClient *avm.Client, xChainWalletClient *avm.WalletClient, addresses []string, balance uint64) (ids.ID, uint64, error) {
	if balance < txFee {
		return ids.ID{}, balance, errors.New("not enough funds left for another transaction")
	}
	maxSendAmount := balance - txFee
	randVal := rand.Float64()
	sendAmount := uint64(float64(maxSendAmount) * randVal)
	sendAmount /= 10000000
	sendAmount *= 10000000
	addrIndex := int(rand.Float32() * float32(len(addresses)))
	address := addresses[addrIndex]
	txID, err := xChainWalletClient.Send(userPass, nil, "", sendAmount, avaxAssetID, address, "")
	if err != nil {
		return ids.ID{}, balance, fmt.Errorf("Failed to send transaction: %s", err)
	}
	logrus.Infof("Sent transaction with ID: %s", txID)

	balance = balance - txFee // Remove the tx fee from burned amount
	return txID, balance, nil
}

func doBurst(xChainClient *avm.Client, xChainWalletClient *avm.WalletClient, addresses []string, balance uint64) (uint64, error) {
	burstSize := int(float32(maxBurstSize) * rand.Float32())
	txIDs := make([]ids.ID, 0, burstSize)
	var (
		txID ids.ID
		err  error
	)
	logrus.Infof("Doing burst of %d transactions", burstSize)
	for i := 0; i < burstSize; i++ {
		txID, balance, err = sendRandomTx(xChainClient, xChainWalletClient, addresses, balance)
		if err != nil {
			return balance, err
		}
		txIDs = append(txIDs, txID)
	}

	logrus.Infof("Confirming burst transactions")
	err = confirmTxs(xChainClient, txIDs)
	return balance, err
}

func sendFundsFor(xChainClient *avm.Client, xChainWalletClient *avm.WalletClient, addresses []string, balance uint64, sendDuration, minSleep, varSleep time.Duration) (uint64, error) {
	end := time.Now().Add(sendDuration)
	for time.Now().Before(end) {
		var err error
		var txID ids.ID
		txID, balance, err = sendRandomTx(xChainClient, xChainWalletClient, addresses, balance)
		if err != nil {
			return balance, err
		}

		status, err := xChainClient.ConfirmTx(txID, 5, time.Second)
		if err != nil {
			return balance, err
		}
		if status != choices.Accepted {
			return 0, fmt.Errorf("transaction %s reported status %s", txID, status)
		}

		totalSleep := minSleep + time.Duration(rand.Float64()*float64(varSleep))
		logrus.Infof("Sleeping for %v. Updated balance: %d", totalSleep, balance)
		time.Sleep(totalSleep)
	}

	return balance, nil
}

func burnFunds() {
	keystoreClient := keystore.NewClient(uri, requestTimeout)
	avmClient := avm.NewClient(uri, "X", requestTimeout)
	avmWalletClient := avm.NewWalletClient(uri, "X", requestTimeout)

	addresses, balance, err := getAddresses(keystoreClient, avmClient)
	if err != nil {
		logrus.Errorf("Failed to get addresses: %s", err)
	}

	for {
		balance, err = doBurst(avmClient, avmWalletClient, addresses, balance)
		if err != nil {
			logrus.Errorf("Failed to complete burst due to: %s", err)
			return
		}
	}
}

func main() {
	for {
		burnFunds()
		time.Sleep(10 * time.Second)
	}
}
