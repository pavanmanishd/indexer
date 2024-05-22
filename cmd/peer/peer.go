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

	var params *chaincfg.Params
	if os.Getenv("CHAIN") == "dogecoin" {
		if os.Getenv("NETWORK") == "mainnet" {
			params = &dogecoin.MainNetParams
		} else {
			params = &dogecoin.TestNet3Params
		}
	} else {
		if os.Getenv("NETWORK") == "mainnet" {
			params = &chaincfg.MainNetParams
		} else {
			params = &chaincfg.TestNet3Params
		}
	}
	store := store.NewStorage(db).SetLogger(logger)
	// fmt.Println(store.GetBlockRangeNBitsGrouped(1,100000,2016))
	syncManager, err := netsync.NewSyncManager(netsync.SyncConfig{
		PeerAddr:    os.Getenv("PEER_URL"),
		ChainParams: params,
		Store:       store,
		Logger:      logger,
	})
	if err != nil {
		panic(err)
	}
	go syncManager.Sync()

	rpcServer := rpc.Default(store, params).SetLogger(logger)
	rpcServer.Run(":"+os.Getenv("RPC_PORT"))
}
