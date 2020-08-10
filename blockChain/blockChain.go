package blockChain

import (
	"BCDns_0.1/dao"
	"BCDns_0.1/messages"
	"BCDns_0.1/utils"
	"bytes"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	//"strings"

	"github.com/boltdb/bolt"
)


const dbFile = "../data/blockchain_%s.db"
const blocksBucket = "blocks"

var (
	BlockChain *Blockchain
	res []string
)

// Blockchain implements interactions with a DB
type Blockchain struct {
	tip []byte
	db  *bolt.DB
}


// CreateBlockchain creates a new blockchain DB
func CreateBlockchain(dbFile string) (*Blockchain, error) {
	var tip []byte

	genesis := NewGenesisBlock()//block
	genesisB := NewBlockValidated(*genesis, map[string][]byte{})//block, signature

	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		fmt.Printf("[CreateBlockchain] error=%v\n", err)
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(blocksBucket))
		if err != nil {
			return err
		}

		bBytes, err := genesisB.MarshalBlock()
		if err != nil {
			fmt.Printf("[CreateBlockchain] genesis.MarshalBinary error=%v\n", err)
			return err
		}
		key, err := genesisB.Hash()
		if err != nil {
			return err
		}
		err = b.Put(key, bBytes)
		if err != nil {
			return err
		}

		err = b.Put([]byte("l"), key)
		if err != nil {
			return err
		}
		tip = key

		return nil
	})
	if err != nil {
		fmt.Printf("[CreateBlockchain] error=%v\n", err)
		return nil, err
	}

	bc := Blockchain{tip, db}
	fmt.Println("jk1111")
	return &bc, nil
}

// NewBlockchain creates a new Blockchain with genesis Block
func NewBlockchain(nodeID string) (*Blockchain, error) {
	dbFile := fmt.Sprintf(dbFile, nodeID)
	if utils.DBExists(dbFile) == false {
		fmt.Println("[NewBlockchain]Blockchain is not exists.")
		return CreateBlockchain(dbFile)
	}
	var tip []byte
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		tip = b.Get([]byte("l")) //得到key（因为只能通过key得到value，所以插入的时候多添加了一条l，key的数据）

		return nil
	})
	if err != nil {
		return nil, err
	}

	bc := Blockchain{tip, db}

	return &bc, nil
}

func (bc *Blockchain) Close() {
	_ = bc.db.Close()
}

//@wuhui start
//todo GetBolckByZoneName(),
//-1.addblock 加个signature --直接使用Blockvalidate中的signature   done
//0.域名-hash存进去 ----done
//1.根据zoneName取hash--done
//2.根据hash取block--done
//3.取出block后在block里面寻找proposal
//3.1.拿出block数据计算签名
//4.将当前签名与block里面的签名进行验证。
//db dao.DB,
//func GetBlockByZoneName(db *dao.DB, zoneName string) (bool, []string,error) {//结构体方法
//	////取出含有hashIndex前缀的数据key，对应的value是blockhash
//	blockhash_new := []byte("") //用于存储blockdata的值（也就是blockvalidate数据类型）
//	fmt.Println(blockhash_new)
//	iter := db.NewIterator(util.BytesPrefix([]byte("hashIndex_")), nil)
//
//	flag := false
//	for iter.Next() {
//		hzoneName := "hashIndex_" + zoneName
//		key := iter.Key()
//		if string(key) != hzoneName { //匹配失败
//			continue
//		} else { //匹配成功
//			flag = true
//			blockhash := iter.Value()   //iter.value的值不能跳出当前循环存在，这个value值是之前存的blockvalidated经过编码的结果
//			blockhash_new = blockhash
//			//todo 这边需要测试一下blockhash_new的值是不是变化了
//			fmt.Println(blockhash)
//			break
//		}
//	}
//	iter.Release()
//	err := iter.Error()
//	if err == nil {
//		return flag, res, nil
//	}
//	//
//	//if flag == false {
//	//	return flag, res, nil
//	//}
//	////需要在blockhash中拿到block
//	//
//	//blockdata, err := UnMarshalBlockValidated(blockhash_new) //blcokdata是Blockvalidated的数据
//	//if err != nil {
//	//	return false, res, nil
//	//}
//	//
//	//for _, msg := range blockdata.ProposalMessages {
//	//	if msg.ZoneName == zoneName {
//	//		value := msg.Values //存储的资源记录值,values是个数组，可能包含有所有的资源记录值
//	//		res = value
//	//		fmt.Println(value)
//	//	}
//	//	old_sig := msg.Signature //消息中已经存在的签名
//	//
//	//	//对proposalmessage重新生成签名
//	//	new_sig, err := msg.Sign2()
//	//	if err != nil {
//	//		return false, res, nil
//	//	}
//	//	if bytes.Compare(old_sig, new_sig) == 0 { //说明比较失败
//	//		return false, res, nil
//	//	}
//	//}
//
//	return true, res, nil
//}

//end @wuhui


func (bc *Blockchain) GetProposalByZoneName(ZoneName string) (*messages.ProposalMessage, error) {
	gp := new(messages.ProposalMessage)
	gerr := errors.New("Not found")

	err := bc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		blockHash := b.Get([]byte(ZoneName))
		blockData := b.Get(blockHash)
		block, err := UnMarshalBlockValidated(blockData) //blockvalidated
		if err != nil {
			return err
		}
		for _, p := range block.Block.ProposalMessages{
			if p.ZoneName == ZoneName && block.VerifyBlockValidated() {//?
				gp = &p
				return nil
			}
		}
		return gerr
	})
	if err != nil {
		return nil, err
	}

	return gp, nil
}

// AddBlock saves the block into the blockchain ...结束整个线程
func (bc *Blockchain) AddBlock(block *BlockValidated) error {
	dao.Dao.Mutex.Lock()
	defer dao.Dao.Mutex.Unlock()
	for _, p := range block.ProposalMessages {
		err := dao.Db.Delete([]byte(p.ZoneName), nil)
		if err != nil {
			fmt.Printf("[AddBlock] Db.Delete error=%v\n", err)
			return err
		}
		ZoneStatePool.Modify(p.ZoneName)
	}
	fmt.Println("[AddBlock] add a new block")
	err := bc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		key, err := block.Hash()
		if err != nil {
			return err
		}
		blockInDb := b.Get(key)
		if blockInDb != nil {
			return nil
		}
		//@wuhui
		for _, p := range block.ProposalMessages {
			fmt.Println("[Addblock] key is", p.ZoneName)
			b.Put([]byte(p.ZoneName), key)
		}
		//end wuhui

		blockData, err := block.MarshalBlockValidated()
		if err != nil {
			return err
		}
		err = b.Put(key, blockData)
		if err != nil {
			return err
		}

		lastHash := b.Get([]byte("l"))
		lastBlockData := b.Get(lastHash)
		lastBlock, err := UnMarshalBlockValidated(lastBlockData)
		if err != nil {
			return err
		}

		if block.Height > lastBlock.Height {
			err = b.Put([]byte("l"), key)
			if err != nil {
				return err
			}
			bc.tip = key
		}

		return nil
	})
	if err != nil {
		fmt.Printf("[AddBlock] error=%v\n", err)
		return err
	}
	return nil
}


//func testBc() {
//	fmt.Println("testBc")
//}

// AddBlock saves the x into the blockchain---更新bolt.db数据库
//func (bc *Blockchain) AddBlock(block *BlockValidated) error { //block+signature
//	dao.Dao.Mutex.Lock()
//	defer dao.Dao.Mutex.Unlock()
//	// hash of block
//	key, err := block.Hash() //将blockhaeder编码成json字符串返回给key
//	if err != nil {
//		return err
//	}
//	for _, p := range block.ProposalMessages {
//		//这两个有啥区别？
//		err := dao.Db.Delete([]byte(p.ZoneName), nil)
//		ZoneStatePool.Modify(p.ZoneName)//将value值置空
//		if err != nil {
//			fmt.Printf("[AddBlock] Db.Delete error=%v\n", err)
//			return err
//		}
//		//@wuhui start
//		//indexkey = hashINdex_zonename,
//		//由于key中不含签名
//		//todo:在leveldb中存储下，用indexkey为key，value = key（也就是blockheader的json编码）
//		indexkey := fmt.Sprintf("hashIndex_%s", p.ZoneName)
//		//dao.Db.Set([]byte(indexkey),key)
//		//由于key中不含签名信息，我将value的值改为存blockdata
//		blockData, err := block.MarshalBlockValidated() //blcokdata是包含了Block结构体和签名的数据的经过json编码后hash // 的值
//		if err != nil {
//			return err
//		}
//		dao.Db.Set([]byte(indexkey),blockData)
//		//@wuhui end
//	}
//	//write boltdb
//	err = bc.db.Update(func(tx *bolt.Tx) error {
//		b := tx.Bucket([]byte(blocksBucket))
//		blockInDb := b.Get(key)
//
//		if blockInDb != nil {
//			return nil
//		}
//
//		blockData, err := block.MarshalBlockValidated() //blcokdata是包含了Block结构体和签名的数据的经过json编码后hash // 的值
//		if err != nil {
//			return err
//		}
//		err = b.Put(key, blockData) //说明节点验证成功，写到bolt数据库
//		if err != nil {
//			return err
//		}
//
//		// @wuhui add zoneName index for leveldb,key represent zoneName and value represent block hash header
//		// 注意一下一个区块中可能含有多条数据
//		//for _, p2 := range block.ProposalMessages {
//		//		//	dao.DB.put([]byte(p2.ZoneName), []byte(key))
//		//		//}
//		// end @wuhui
//
//		// 取block参照这个
//		lastHash := b.Get([]byte("l"))
//		lastBlockData := b.Get(lastHash)
//		lastBlock, err := UnMarshalBlockValidated(lastBlockData)
//		if err != nil {
//			return err
//		}
//
//		if block.Height > lastBlock.Height {
//			err = b.Put([]byte("l"), key)
//			if err != nil {
//				return err
//			}
//			bc.tip = key
//		}
//
//		return nil
//	})
//	if err != nil {
//		fmt.Printf("[AddBlock] error=%v\n", err)
//		return err
//	}
//	return nil
//}

// FindTransaction finds a transaction by its ID
func (bc *Blockchain) FindProposal(ID []byte) (messages.ProposalMessage, error) {
	bci := bc.Iterator() //拿到tip和boltdb

	for {
		block, err := bci.Next()
		if err != nil {
			return messages.ProposalMessage{}, err
		}

		for _, p := range block.ProposalMessages {
			if bytes.Compare(p.Id, ID) == 0  {
				return p, nil
			}
		}

		if len(block.PrevBlock) == 0 {
			break
		}
	}

	return messages.ProposalMessage{}, errors.New("Transaction is not found")
}

// FindTransaction finds a transaction by its ID
// Iterator returns a BlockchainIterat
func (bc *Blockchain) Iterator() *Iterator {
	bci := &Iterator{bc.tip, bc.db}

	return bci
}

// GetLatestBlock returns the latest block
func (bc *Blockchain) GetLatestBlock() (*BlockValidated, error) {
	lastBlock := new(BlockValidated)
	var err error

	err = bc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		lastHash := b.Get([]byte("l"))
		blockData := b.Get(lastHash)
		lastBlock, err = UnMarshalBlockValidated(blockData)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return lastBlock, nil
}

func (bc *Blockchain) RevokeBlock() error {
	lastBlock, err := bc.GetLatestBlock()
	if err != nil {
		return err
	}
	err = bc.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		prevHash := lastBlock.PrevBlock
		key, err := lastBlock.Hash()
		if err != nil {
			return err
		}

		err = b.Put([]byte("l"), prevHash)
		if err != nil {
			return err
		}
		bc.tip = prevHash

		err = b.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// GetBlockByHash finds a block by its  hash and returns it
func (bc *Blockchain) GetBlockByHash(blockHash []byte) (*BlockValidated, error) {
	block := new(BlockValidated)
	var err error

	err = bc.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		blockData := b.Get(blockHash)

		if blockData == nil {
			return errors.New("Block is not found.")
		}

		block, err = UnMarshalBlockValidated(blockData)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}

// GetBlockByHeight finds a block by its height and returns it
func (bc *Blockchain) GetBlockByHeight(h uint) (*BlockValidated, error) {
	lastBlock, err := bc.GetLatestBlock()
	if err != nil {
		return nil, err
	}
	if lastBlock.Height < h {
		return nil, errors.New("[GetBlockByHeight] %v out of height")
	}
	bci := bc.Iterator()

	for {
		block, err := bci.Next()
		if err != nil {
			return nil, err
		}
		if block.Height == h {
			return block, nil
		}

	}
}

// GetBlockHashes returns a list of hashes of all the blocks in the chain
func (bc *Blockchain) GetBlockHashes() [][]byte {
	var blocks [][]byte
	bci := bc.Iterator()

	for {
		block, err := bci.Next()
		if err != nil {
			return nil
		}

		hash, err := block.Hash()
		if err != nil {
			continue
		}
		blocks = append(blocks, hash)

		if len(block.PrevBlock) == 0 {
			break
		}
	}

	return blocks
}

// MineBlock mines a new block with the provided transactions
func (bc *Blockchain) MineBlock(proposals messages.ProposalMessages) (*Block, error) {
	block, err := bc.GetLatestBlock()
	if err != nil {
		return nil, err
	}
	lastHash, err := block.Hash()
	if err != nil {
		return nil, err
	}

	newBlock := NewBlock(proposals, lastHash, block.Height+1, false)
	if newBlock == nil {
		return nil, errors.New("[MineBlock] NewBlock failed")
	}

	return newBlock, nil
}

//@wuhui 在区块里面找到域名对应的proposal消息
func (bc *Blockchain) FindDomain(name string) (*messages.ProposalMessage, error) {
	bci := bc.Iterator()

	for {
		block, err := bci.Next()
		if err != nil {
			return nil, err
		}

		if p := block.ProposalMessages.FindByZoneName(name); p != nil {
			return p, nil
		}

		if len(block.PrevBlock) == 0 {
			break
		}
	}
	return nil, nil
}

func (bc *Blockchain) Get(key []byte) ([]byte, error) {
	bci := bc.Iterator()

	for {
		block, err := bci.Next()
		if err != nil {
			return nil, err
		}
		ps := ReverseSlice(block.ProposalMessages)
		for _, p := range ps {
			if p.ZoneName == string(key) {
				data, err := p.MarshalProposalMessage()
				if err != nil {
					return nil, err
				}
				return data, nil
			}
		}

		if len(block.PrevBlock) == 0 {
			break

		}
	}
	return nil, leveldb.ErrNotFound
}

func (bc *Blockchain) Set(key, value []byte) error {
	return nil
}

func ReverseSlice(s messages.ProposalMessages) messages.ProposalMessages {
	ss := make(messages.ProposalMessages, len(s))
	for i, j := 0, len(s)-1; i <= j; i, j = i+1, j-1 {
		ss[i], ss[j] = s[j], s[i]
	}
	return ss
}