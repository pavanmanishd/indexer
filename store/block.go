package store

import (
	"fmt"
	"strconv"
	// "strings"
	"sort"
	"sync"
	"github.com/catalogfi/indexer/model"
)

var (
	latestBlockHeightKey = "latestBlockHeight"
	orphanKey            = "orphan"
)

// GetLatestBlockHeight returns the latest block height in the database
func (s *Storage) GetLatestBlockHeight() (uint64, bool, error) {
	data, err := s.db.Get(latestBlockHeightKey)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	height, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, false, fmt.Errorf("GetLatestBlockHeight: error converting height to int: %w", err)
	}
	return uint64(height), true, nil
}

func (s *Storage) GetLatestTipHash() (string, bool, error) {
	height, exists, err := s.GetLatestBlockHeight()
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "", false, nil
	}
	block, exists, err := s.GetBlockByHeight(height)
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "", false, nil
	}
	return block.Hash, true, nil
}

func (s *Storage) SetLatestBlockHeight(height uint64) error {
	heightStr := strconv.Itoa(int(height))
	return s.db.Put(latestBlockHeightKey, []byte(heightStr))
}

// GetBlocks returns the blocks with the given heights.
func (s *Storage) GetBlocks(heights []uint64) ([]*model.Block, error) {
	blocks := make([]*model.Block, 0)
	for _, height := range heights {
		data, err := s.db.Get(fmt.Sprint(height))
		if err != nil {
			return nil, err
		}
		block, err := model.UnmarshalBlock(data)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil

}

func (s *Storage) GetBlock(hash string) (*model.Block, bool, error) {
	data, err := s.db.Get(hash)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	block, err := model.UnmarshalBlock(data)
	if err != nil {
		return nil, false, fmt.Errorf("GetBlock: error unmarshalling block: %w", err)
	}
	return block, true, nil

}

func (s *Storage) GetBlockByHeight(height uint64) (*model.Block, bool, error) {
	data, err := s.db.Get(fmt.Sprint(height))
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	block, err := model.UnmarshalBlock(data)
	if err != nil {
		return nil, false, fmt.Errorf("GetBlockByHeight: error unmarshalling block: %w", err)
	}
	return block, true, nil
}

func (s *Storage) GetOrphanBlockByHeight(height uint64) (*model.Block, bool, error) {
	key := fmt.Sprintf("%s_%d", orphanKey, height)
	data, err := s.db.Get(key)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	block, err := model.UnmarshalBlock(data)
	if err != nil {
		return nil, false, fmt.Errorf("GetOrphanBlockByHeight: error unmarshalling block: %w", err)
	}
	return block, true, nil
}

func (s *Storage) GetBlocksRange(start, end uint64, areOrphans bool) ([]*model.Block, error) {
	blocks := make([]*model.Block, 0)
	for i := start; i <= end; i++ {
		var block *model.Block
		var err error
		var exists bool
		if areOrphans {
			block, exists, err = s.GetOrphanBlockByHeight(i)
		} else {
			block, exists, err = s.GetBlockByHeight(i)
		}
		if err != nil {
			return nil, err
		}
		if exists {
			blocks = append(blocks, block)
		}
	}
	return blocks, nil
}

func (s *Storage) BlockExists(hash string) (bool, error) {
	_, err := s.db.Get(hash)
	if (err != nil && err.Error() == ErrKeyNotFound) || err != nil {
		return false, nil
	}
	return true, err
}

func (s *Storage) GetOrphanBlock(hash string) (block *model.Block, exists bool, err error) {
	key := fmt.Sprintf("%s_%s", orphanKey, hash)
	data, err := s.db.Get(key)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return nil, false, nil
		}
	}
	block, err = model.UnmarshalBlock(data)
	if err != nil {
		return nil, false, err
	}
	return block, true, nil
}

func (s *Storage) GetBlockTxs(blockHash string, isOrphan bool) ([]*model.Transaction, error) {
	key := blockHash
	if isOrphan {
		key = fmt.Sprintf("%s_%s", orphanKey, blockHash)
	}
	data, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	block, err := model.UnmarshalBlock(data)
	if err != nil {
		return nil, err
	}
	return s.GetTxs(block.Txs)

}

func (s *Storage) PutOrphanBlock(block *model.Block) error {
	blockInBytes, err := block.Marshal()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s_%s", orphanKey, block.Hash)
	if err = s.db.Put(key, blockInBytes); err != nil {
		return err
	}
	key = fmt.Sprintf("%s_%d", orphanKey, block.Height)
	return s.db.Put(key, blockInBytes)
}

func (s *Storage) PutBlock(block *model.Block) error {
	blockInBytes, err := block.Marshal()
	if err != nil {
		return err
	}
	err = s.db.Put(fmt.Sprint(block.Height), blockInBytes)
	if err != nil {
		return err
	}
	return s.db.Put(block.Hash, blockInBytes)
}

func (s *Storage) RemoveBlock(hash string) error {
	block, exists, err := s.GetBlock(hash)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	err = s.db.Delete(hash)
	if err != nil {
		return err
	}
	return s.db.Delete(fmt.Sprint(block.Height))
}

func (s *Storage) RemoveBlocksAbove(hash string) error {
	height, exists, err := s.GetBlockHeight(hash)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	for i := height; i > 0; i-- {
		err = s.db.Delete(fmt.Sprint(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) GetBlockHeight(hash string) (uint64, bool, error) {
	data, err := s.db.Get(hash)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	block, err := model.UnmarshalBlock(data)
	if err != nil {
		return 0, false, fmt.Errorf("GetBlockHeight: error unmarshalling block: %w", err)
	}
	return block.Height, true, nil
}

func (s *Storage) GetBlockRangeUniqueNBits(start uint64,end uint64) (int, error) {
	uniqueElements := make(map[uint64]struct{})
	var result []uint64
	for i:=start;i<=end;i++ {
		block,exists,err := s.GetBlockByHeight(i)
        if err!=nil {
			return 0,err
		}
		if !exists {
			break
		}
		uniqueElements[uint64(block.Bits)] = struct{}{}
    }

    for num := range uniqueElements {
        result = append(result, num)
    }
	fmt.Println(result)
    return len(result), nil
}

type BatchResult struct {
    BatchNum int
    Result   int
}

func (s *Storage) GetBlockRangeNBitsGrouped(start uint64, end uint64, group uint64) ([]int, error) {
    diff := end - start + 1
    var curr uint64 = start
    var results []BatchResult
    var wg sync.WaitGroup
    var mu sync.Mutex

    for diff > 0 {
        if diff < group {
            group = diff
        }

        wg.Add(1)
        go func(curr, group uint64) {
            defer wg.Done()

            unique, err := s.GetBlockRangeUniqueNBits(curr, curr+group-1)
            if err != nil {
                // Handle error
                return
            }

            mu.Lock()
            defer mu.Unlock()
            results = append(results, BatchResult{BatchNum: int(curr), Result: unique})
        }(curr, group)

        diff -= group
        curr += group
    }

    wg.Wait()

    // Sort the results by batch number
    sort.Slice(results, func(i, j int) bool {
        return results[i].BatchNum < results[j].BatchNum
    })

    // Extract the results in order
    var finalResult []int
    for _, res := range results {
        finalResult = append(finalResult, res.Result)
    }

    return finalResult, nil
}
