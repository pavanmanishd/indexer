package main

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/catalogfi/indexer/model"
	"github.com/catalogfi/indexer/peer"
	"github.com/catalogfi/indexer/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	db, err := model.NewDB(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	str := store.NewStorage(&chaincfg.RegressionNetParams, db)

	p, err := peer.NewPeer("127.0.0.1:18444", str)
	if err != nil {
		panic(err)
	}
	p.Run()
}
