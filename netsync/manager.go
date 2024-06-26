package netsync

import (
	"context"
	"fmt"
	"time"
	"os"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"

	"github.com/catalogfi/indexer/mempool"
	"github.com/catalogfi/indexer/model"
	"github.com/catalogfi/indexer/store"
	"github.com/catalogfi/indexer/utils"
	"go.uber.org/zap"
)

type SyncManager struct {
	peer         *Peer //TODO: will we have multiple peers in future?
	mempool      *mempool.Mempool
	store        *store.Storage
	chainParams  *chaincfg.Params
	latestHeight uint64
	isSynced     bool
	isMempoolSynced bool
	logger       *zap.Logger
}

type SyncConfig struct {
	PeerAddr    string
	ChainParams *chaincfg.Params
	Store       *store.Storage
	Logger      *zap.Logger
}

func NewSyncManager(config SyncConfig) (*SyncManager, error) {

	logger := config.Logger.Named("syncManager")
	peer, err := NewPeer(config.PeerAddr, config.ChainParams, logger)
	if err != nil {
		return nil, err
	}

	latestHeight, _, err := config.Store.GetLatestBlockHeight()
	if err != nil {
		return nil, err
	}

	return &SyncManager{
		peer:         peer,
		chainParams:  config.ChainParams,
		logger:       logger,
		store:        config.Store,
		latestHeight: latestHeight,
		mempool:      mempool.New(config.Store),
	}, nil
}

func (s *SyncManager) Sync() error {
	if err := s.checkForGensisBlock(); err != nil {
		return err
	}

	for {
		ctx, cancel := context.WithCancel(context.Background())

		closed := s.peer.OnMsg(ctx, func(msg interface{}) error {
			if s.isSynced && !s.isMempoolSynced {
				go func() {
					s.logger.Info("syncing all mempool transactions...")
					err := s.mempool.SyncMempool(os.Getenv("RPC_URL"), os.Getenv("RPC_USER"), os.Getenv("RPC_PASS"))
					if err != nil {
						s.logger.Error("mempool sync error", zap.Error(err))
						s.isMempoolSynced = false
					}
				}()
				s.isMempoolSynced = true
			}
			switch m := msg.(type) {
			case *wire.MsgBlock:
				block := m
				if err := s.putBlock(block); err != nil {
					s.logger.Error("sync: ", zap.String("hash", block.BlockHash().String()), zap.Error(err))
					return err
				}
				return nil
			case *wire.MsgTx:
				tx := m
				s.logger.Info("received tx", zap.String("txid", tx.TxHash().String()))
				if err := s.putMempoolTx(tx); err != nil {
					s.logger.Error("sync: ", zap.String("txid", tx.TxHash().String()), zap.Error(err))
					return err
				}
			}
			return nil
		})

		go s.fetchBlocks()
		s.peer.WaitForDisconnect()
		cancel()

		<-closed
		s.logger.Warn("peer got disconnected... reconnecting")
		reconnectedPeer, err := s.peer.Reconnect()
		if err != nil {
			s.logger.Error("error reconnecting peer", zap.Error(err))
			panic(err)
		}
		s.peer = reconnectedPeer
	}
}

func (s *SyncManager) putMempoolTx(tx *wire.MsgTx) error {
    // Get the latest block height from the store
    latest, _, err := s.store.GetLatestBlockHeight()
    if err != nil {
        return err
    }
    
    // If the latest block height is not available or if the latest block height
    // is different from the last block height reported by the peer,
    // then return without processing the mempool transaction
    if latest != 0 && latest != uint64(s.peer.LastBlock()) {
        // We don't process mempool txs until the blockchain is completely synced
        return nil
    }
    
    return s.mempool.ProcessTx(tx)
}

func (s *SyncManager) fetchBlocks() {
	for {
		// Check if the peer is connected
		if !s.peer.Connected() {
			s.logger.Info("peer disconnected")
			break
		}

		// Get the latest block height from the store
		latestBlockHeight, _, err := s.store.GetLatestBlockHeight()
		if err != nil {
			s.logger.Error("error getting latest block height", zap.Error(err))
			continue
		}

		peerLastBlock := s.peer.LastBlock()
		s.logger.Info("latest block height", zap.Uint64("latestBlockHeight", latestBlockHeight), zap.Int32("peerLastBlock", peerLastBlock))

		// Check if the peer's last block is valid
		if peerLastBlock == 0 {
			s.logger.Warn("peer's last block is 0, waiting for peer to synchronize")
			continue
		}

		// Check if the blockchain is already synced
		if latestBlockHeight == uint64(peerLastBlock) && latestBlockHeight != 0 {
			s.logger.Info("blockchain synced ✅")
			s.setSyncedStatus(true)
			return
		}

		// Get block locator
		locator, err := s.getBlockLocator(latestBlockHeight)
		if err != nil {
			s.logger.Error("error getting block locator", zap.Error(err))
			continue
		}

		// Push getblocks message to peer
		if err := s.peer.PushGetBlocksMsg(locator, &chainhash.Hash{}); err != nil {
			s.logger.Error("error pushing getblocks message", zap.Error(err))
			continue
		}

		// Wait for blocks to be processed
		if s.waitForBlocksToBeProcessed(locator) {
			s.logger.Info("blocks processed")
		} else {
			s.logger.Info("blockchain synced ✅")
			s.setSyncedStatus(true)
			return
		}
	}
}

// setSyncedStatus sets the synced status for both the SyncManager and its peer
func (s *SyncManager) setSyncedStatus(status bool) {
	s.isSynced = status
	if s.peer != nil {
		s.peer.isSynced = status
	}
}


// while syncing, we need to make sure all requested blocks ..
// are processed first and then only we request for more blocks
// everytime we request, 500 new blocks are received
// so we wait for 500 blocks to be processed.
// if the blockchain is completely synced,
// then peers send the mined block(we don't request for it, it's just sent to us)
// In that case, below function will wait for next 501 blocks to be processed
// and then callee function should handle the case where the blockchain is completely synced
func (s *SyncManager) waitForBlocksToBeProcessed(locator []*chainhash.Hash) bool {
    // Set a default limit
    limit := 501
    
    // Calculate the difference between the last block and the latest block height
    diff := int(s.peer.LastBlock() - int32(s.latestHeight))
    
    // Check if the latest block height is non-zero and the difference is zero,
    // which means all blocks have been processed
    if s.latestHeight != 0 && diff == 0 {
        return false
    }
    
    // Adjust limit if the difference is within a reasonable range
    if diff > 0 && diff < 500 {
        limit = diff
    }
    
    // If the locator is empty, set limit to 500
    if len(locator) == 0 {
        limit = 500
    }
    
    // Iterate up to the limit
    for i := 0; i < limit; i++ {
        // Recalculate the difference inside the loop to reflect any changes
        diff = int(s.peer.LastBlock() - int32(s.latestHeight))
		
        // Check if the latest block height is non-zero and the difference is zero,
        // which means all blocks have been processed
        if s.latestHeight != 0 && diff == 0 {
            <-s.peer.blockProcessed
			return false
        }
		
		// Wait for a block to be processed
		<-s.peer.blockProcessed
        
    }
    
    // All blocks up to the limit have been processed
    return true
}


func (s *SyncManager) getBlockLocator(latestBlockHeight uint64) ([]*chainhash.Hash, error) {

	locatorIDs := calculateLocator(latestBlockHeight)
	blocks, err := s.store.GetBlocks(locatorIDs)
	if err != nil && err.Error() != store.ErrKeyNotFound {
		return nil, err
	}
	hashes := make([]*chainhash.Hash, len(blocks))
	for i := range blocks {
		hash, err := chainhash.NewHashFromStr(blocks[i].Hash)
		if err != nil {
			return hashes, err
		}
		hashes[i] = hash
	}

	return hashes, nil
}

func (s *SyncManager) checkForGensisBlock() error {
	_, exists, err := s.store.GetBlock(s.chainParams.GenesisBlock.BlockHash().String())
	if err != nil {
		return err
	}
	if !exists {
		err := s.putGensisBlock(s.chainParams.GenesisBlock)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SyncManager) putBlock(block *wire.MsgBlock) error {

	height := uint64(0)
	// we check if w already have the block

	exists, err := s.store.BlockExists(block.BlockHash().String())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	//handle orphan blocks
	_, exists, err = s.store.GetOrphanBlock(block.BlockHash().String())
	if err != nil {
		return err
	}
	if exists {
		// we already have the orphan block too, so just ignore it
		return nil
	}

	previousBlock, exists, err := s.store.GetBlock(block.Header.PrevBlock.String())
	if err != nil {
		return err
	}

	if !exists {
		orphanBlock, exists, err := s.store.GetOrphanBlock(block.Header.PrevBlock.String())
		if err != nil {
			return err
		}
		if exists {
			if s.latestHeight <= orphanBlock.Height+1 {
				// the block we got might not be orphan anymore
				// reorganize the blocks
				fmt.Println(s.latestHeight , orphanBlock.Height)
				for s.latestHeight <= orphanBlock.Height {
					// get the previous block of the orphan block
					previousBlock, exists, err := s.store.GetBlock(orphanBlock.PreviousBlock)
					if err != nil {
						return err
					}
					if !exists {
						// we don't have the previous block in the main chain or orphan chain
						// do not process the block
						return nil
					}
					orphanBlock = previousBlock
				}
				err := s.reorganizeBlocks(orphanBlock)
				if err != nil {
					return err
				}
				//proceed with the current block
			} else {
				// we don't have the previous block in the main chain or orphan chain
				// do not process the block
				return s.putOrphanBlock(block, orphanBlock.Height+1)
			}
		} else {
			// we don't have the previous block in the main chain or orphan chain
			// do not process the block
			return nil
		}
	}

	if s.latestHeight >= previousBlock.Height+1 {
		return s.putOrphanBlock(block, previousBlock.Height+1)
	}

	height = previousBlock.Height + 1
	// s.logger.Info("processing block", zap.Uint64("height", height), zap.String("hash", block.BlockHash().String()))

	txHashes := make([]string, len(block.Transactions))
	for i, tx := range block.Transactions {
		txHashes[i] = tx.TxHash().String()
	}
	newBlock := model.Block{
		Hash:   block.Header.BlockHash().String(),
		Height: height,

		IsOrphan:      false,
		PreviousBlock: block.Header.PrevBlock.String(),
		Version:       block.Header.Version,
		Nonce:         block.Header.Nonce,
		Timestamp:     block.Header.Timestamp,
		Bits:          block.Header.Bits,
		MerkleRoot:    block.Header.MerkleRoot.String(),
		Txs:           txHashes,
	}
	if err := s.store.PutBlock(&newBlock); err != nil {
		s.logger.Error("error putting block with hash", zap.Error(err))
		return err
	}

	vouts, vins, txIns, transactions, err := utils.SplitTxs(block.Transactions, block.BlockHash().String())
	if err != nil {
		return err
	}

	err = s.store.PutUTXOs(vouts)
	if err != nil {
		s.logger.Error("error putting utxos", zap.Error(err))
		return err
	}

	timeNow := time.Now()
	s.logger.Info("putting raw txs")
	err = s.store.PutTxs(transactions)
	if err != nil {
		s.logger.Error("error putting transactions", zap.Error(err))
		return err
	}
	s.logger.Info("putting raw txs done", zap.Duration("time", time.Since(timeNow)))

	timeNow = time.Now()
	hashes := make([]string, 0)
	indices := make([]uint32, 0)
	s.logger.Info("removing utxos")
	for _, in := range txIns {
		if in.PreviousOutPoint.Hash.String() == "0000000000000000000000000000000000000000000000000000000000000000" {
			continue
		}
		hashes = append(hashes, in.PreviousOutPoint.Hash.String())
		indices = append(indices, in.PreviousOutPoint.Index)
	}
	s.logger.Info("removing utxos step 2", zap.Int("len hashes", len(hashes)),zap.Int("len indices", len(indices)),zap.Int("len vins",len(vins)))
	//Ignores the coinbase transaction
	if len(vins) > 0 {
    err = s.store.RemoveUTXOs(hashes, indices, vins[1:])
	  if err != nil {
		  s.logger.Error("error removing utxos", zap.Error(err))
		  return err
	  }
  }
	s.logger.Info("removing utxos done", zap.Duration("time", time.Since(timeNow)))

	if err := s.store.SetLatestBlockHeight(height); err != nil {
		return err
	}
	s.logger.Info("successfully block indexed", zap.Uint64("height", height))
	s.latestHeight = height
	s.peer.UpdateLastBlockHeight(int32(height))
	return nil
}

func (s *SyncManager) putOrphanBlock(block *wire.MsgBlock, height uint64) error {
	txHashes := make([]string, len(block.Transactions))
	for i, tx := range block.Transactions {
		txHashes[i] = tx.TxHash().String()
	}
	orphanBlock := model.Block{
		Hash:          block.Header.BlockHash().String(),
		Height:        height,
		IsOrphan:      true,
		PreviousBlock: block.Header.PrevBlock.String(),
		Version:       block.Header.Version,
		Nonce:         block.Header.Nonce,
		Timestamp:     block.Header.Timestamp,
		Bits:          block.Header.Bits,
		MerkleRoot:    block.Header.MerkleRoot.String(),
		Txs:           txHashes,
	}
	if err := s.store.PutOrphanBlock(&orphanBlock); err != nil {
		return err
	}

	_, _, _, transactions, err := utils.SplitTxs(block.Transactions, block.BlockHash().String())
	if err != nil {
		return err
	}
	if err = s.store.PutTxs(transactions); err != nil {
		return err
	}
	//we do not put utxos for orphan blocks
	return nil
}

// goal is to travel back to common ancestor of orphan chain and the main chain
// and then reorganize the blocks such that the orphan chain becomes the main chain
// and the main chain becomes the orphan chain (from the common ancestor)
func (s *SyncManager) reorganizeBlocks(orphanBlock *model.Block) error {
	// get the common ancestor of the orphan chain and the main chain
	commonAncestor, exists, err := s.store.GetBlockByHeight(orphanBlock.Height)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("common ancestor block not found")
	}

	// get all blocks from the main chain from the common ancestor
	mainChainBlocks, err := s.store.GetBlocksRange(commonAncestor.Height+1, s.latestHeight, false)
	if err != nil {
		return err
	}

	// get all blocks from the orphan chain from the orphan block
	orphanChainBlocks, err := s.store.GetBlocksRange(orphanBlock.Height+1, s.latestHeight, true)
	if err != nil {
		return err
	}

	// remove all blocks from the main chain from the common ancestor
	// and put them in the orphan chain
	for _, block := range mainChainBlocks {
		if err := s.orphanBlock(block); err != nil {
			return err
		}
	}

	// remove all blocks from the orphan chain from the orphan block
	// and put them in the main chain
	for _, block := range orphanChainBlocks {
		if err := s.unorphanBlock(block); err != nil {
			return err
		}
	}
	s.latestHeight = orphanBlock.Height
	s.peer.UpdateLastBlockHeight(int32(orphanBlock.Height))

	return nil

}

func (s *SyncManager) unorphanBlock(block *model.Block) error {
	block.IsOrphan = false
	txs, err := s.store.GetBlockTxs(block.Hash, true)
	if err != nil {
		return err
	}
	vouts := make([]model.Vout, 0)
	for _, tx := range txs {
		for _, vout := range tx.Vouts {
			vouts = append(vouts, vout)
		}
	}
	if err := s.store.PutUTXOs(vouts); err != nil {
		return err
	}
	return s.store.PutBlock(block)
}

func (s *SyncManager) orphanBlock(block *model.Block) error {
	block.IsOrphan = true
	txs, err := s.store.GetBlockTxs(block.Hash, false)
	if err != nil {
		return err
	}
	hashes := make([]string, 0)
	indices := make([]uint32, 0)
	vins := make([]model.Vin, 0)
	for _, tx := range txs {
		vins = append(vins, tx.Vins...)
		for _, vin := range tx.Vouts {
			hashes = append(hashes, vin.TxId)
			indices = append(indices, vin.Index)
		}
	}

	if err := s.store.RemoveUTXOs(hashes, indices, vins); err != nil {
		return err
	}

	return s.store.PutOrphanBlock(block)
}

func (s *SyncManager) putGensisBlock(block *wire.MsgBlock) error {
	genesisBlock := btcutil.NewBlock(s.chainParams.GenesisBlock)
	genesisBlock.SetHeight(0)

	genBlock := &model.Block{
		Hash:          genesisBlock.Hash().String(),
		Height:        0,
		IsOrphan:      false,
		PreviousBlock: genesisBlock.MsgBlock().Header.PrevBlock.String(),
		Version:       genesisBlock.MsgBlock().Header.Version,
		Nonce:         genesisBlock.MsgBlock().Header.Nonce,
		Timestamp:     genesisBlock.MsgBlock().Header.Timestamp,
		Bits:          genesisBlock.MsgBlock().Header.Bits,
		MerkleRoot:    genesisBlock.MsgBlock().Header.MerkleRoot.String(),
		Txs:           []string{"0000000000000000000000000000000000000000000000000000000000000000"},
	}
	if err := s.store.PutBlock(genBlock); err != nil {
		return err
	}

	tx := &model.Transaction{
		Hash: "0000000000000000000000000000000000000000000000000000000000000000",
	}
	if err := s.store.PutTx(tx); err != nil {
		return err
	}
	if err := s.store.SetLatestBlockHeight(0); err != nil {
		return err
	}
	return nil
}

// refer to https://en.bitcoin.it/wiki/Protocol_documentation#getblocks
func calculateLocator(topHeight uint64) []uint64 {
	start := int64(topHeight)
	var indexes []uint64
	// Modify the step in the iteration.
	step := int64(1)
	// Start at the top of the chain and work backwards.
	for index := start; index > 0; index -= step {
		// Push top 10 indexes first, then back off exponentially.
		if len(indexes) >= 10 {
			step *= 2
		}
		indexes = append(indexes, uint64(index))
	}

	// Push the genesis block index.
	indexes = append(indexes, 0)
	return indexes
}

func (s *SyncManager) RemoveBlock(height uint64) error {
	block, exists, err := s.store.GetBlockByHeight(height)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("block not found")
	}
	if err := s.store.RemoveBlocksAbove(block.Hash); err != nil {
		return err
	}
	return nil
}
