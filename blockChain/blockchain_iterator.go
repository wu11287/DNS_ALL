package blockChain

import (
	"fmt"
	"github.com/boltdb/bolt"
)

// BlockchainIterator is used to iterate over blockchain blocks
type Iterator struct {
	currentHash []byte
	db          *bolt.DB
}

// Next returns next block starting from the tip
func (i *Iterator) Next() (*BlockValidated, error) {
	var (
		block *BlockValidated
		err   error
	)

	err = i.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		encodedBlock := b.Get(i.currentHash)
		block, err = UnMarshalBlockValidated(encodedBlock)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		fmt.Printf("[Next] error=%v\n", err)
		return nil, err
	}

	i.currentHash = block.PrevBlock

	return block, nil
}
