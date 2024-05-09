package main

import (
	// "fmt"
	"os"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/catalogfi/indexer/database"
	"github.com/catalogfi/indexer/netsync"
	"github.com/catalogfi/indexer/rpc"
	"github.com/catalogfi/indexer/store"
	"github.com/catalogfi/indexer/dogecoin"
	"go.uber.org/zap"
)



func main() {

	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"stdout"}
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	dbPath := os.Getenv("DB_PATH")

	db, err := database.NewRocksDB(dbPath, logger)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var dogeCoinNetwork *chaincfg.Params
	if os.Getenv("NETWORK") == "mainnet" {
		dogeCoinNetwork = &dogecoin.MainNetParams
	} else {
		dogeCoinNetwork = &dogecoin.TestNet3Params
	}

	store := store.NewStorage(db).SetLogger(logger)
	// fmt.Println(store.GetBlockRangeNBitsGrouped(1,100000,2016))
	syncManager, err := netsync.NewSyncManager(netsync.SyncConfig{
		PeerAddr:    os.Getenv("PEER_URL"),
		ChainParams: dogeCoinNetwork,
		Store:       store,
		Logger:      logger,
	})
	if err != nil {
		panic(err)
	}
	go syncManager.Sync()

	rpcServer := rpc.Default(store, dogeCoinNetwork).SetLogger(logger)
	rpcServer.Run(":8080")
}
