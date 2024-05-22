package command

import (
	"encoding/json"

	"github.com/catalogfi/indexer/store"
)

type latestTip struct {
	store *store.Storage
}

func (l *latestTip) Name() string {
	return "latest_tip"
}

func (l *latestTip) Execute(params json.RawMessage) (interface{}, error) {
	height, exists, err := l.store.GetLatestBlockHeight()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrGetLatestBlockHeightNone
	}
	return height, nil
}

func LatestTip(store *store.Storage) Command {
	return &latestTip{
		store: store,
	}
}

// latestTipHash

type latestTipHash struct {
	store *store.Storage
}

func (l *latestTipHash) Name() string {
	return "latest_tip_hash"
}

func (l *latestTipHash) Execute(params json.RawMessage) (interface{}, error) {
	hash, exists, err := l.store.GetLatestTipHash()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrGetLatestTipHash
	}
	return hash, nil
}

func LatestTipHash(store *store.Storage) Command {
	return &latestTipHash{
		store: store,
	}
}


// get_block_by_height

type getBlockByHeight struct {
	store *store.Storage
}

func (g *getBlockByHeight) Name() string {
	return "get_block_by_height"
}

func (g *getBlockByHeight) Execute(params json.RawMessage) (interface{}, error) {
	var height int64
	if err := json.Unmarshal(params, &height); err != nil {
		return nil, err
	}
	block, exists, err := g.store.GetBlockByHeight(uint64(height))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrGetBlockNotFound
	}
	return block, nil
}

func GetBlockByHeight(store *store.Storage) Command {
	return &getBlockByHeight{
		store: store,
	}
}