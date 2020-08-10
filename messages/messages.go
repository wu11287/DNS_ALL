package messages

import (
	"BCDns_0.1/bcDns/conf"
	"BCDns_0.1/certificateAuthority/service"
	"BCDns_0.1/dao"
	"BCDns_0.1/utils"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"time"
	"strconv"
)

var (
	logger *logging.Logger // package-level logger
	FFFcount uint
)

type OperationType uint8

const (
	Add OperationType = iota
	Del
	Mod
	Select
)

const Dereliction = "No owner"

type ProposalMessage struct {
	utils.Base
	Type      OperationType
	ZoneName  string
	Owner     string
	Values    []string
	Nonce     uint32
	Id        []byte
	Signature []byte
	ISValidHost bool
}

func init() {
	logger = logging.MustGetLogger("messages")
}

func NewProposal(zoneName string, t OperationType, values []string) *ProposalMessage {
	var (
		err  error
		base = utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		}
		msg ProposalMessage
	)
	fmt.Println("NewProposal")
	switch t {
	case Add:
		msg = ProposalMessage{
			Base:     base,
			Type:     Add,
			ZoneName: zoneName,
			Owner:    base.From,
			Values:   values,
		}
		err = msg.GetPow()
		if err != nil {
			fmt.Printf("[NewProposal] GetPowHash Failed err=%v\n", err)
			return nil
		}
	case Del:
		msg = ProposalMessage{
			Base:     base,
			Type:     Del,
			ZoneName: zoneName,
			Owner:    Dereliction,
			Values:   values,
		}
	case Select:
		// blockProposal := new(ProposalMessage)
		// if data, err := dao.Dao.GetZoneName(zoneName); err == leveldb.ErrNotFound {
		// 	fmt.Printf("[ValidateMod] ZoneName is not exist\n")
		// 	return nil
		// } else {
		// 	blockProposal, err = UnMarshalProposalMessage(data)
		// 	if err != nil {
		// 		fmt.Printf("[NewProposal] UnMarshalProposalMassage error=%v\n", err)
		// 		return nil
		// 	}
		// 	if blockProposal.From == Dereliction {
		// 		fmt.Printf("[ValidateMod] ZoneName is not exist\n")
		// 		return nil
		// 	}
		// }
		ip := values[0]
		t , _ := strconv.Atoi(values[1])
		fmt.Println("host:", zoneName, "ip", ip)
		f := false
		blockProposal := new(ProposalMessage)
		te := ""

		if t == 5 {
			te = "CNAME"
		}else if t == 1 {
			te = "A"
		}else if t == 27 {
			te = "AAAA"
		}
		rr := zoneName + "IN" + te + ip


		if data, err := dao.Dao.GetZoneName(rr); err == leveldb.ErrNotFound {
			fmt.Printf("[ValidateMod] ZoneName is not exist\n")
			return nil
		} else {
			blockProposal, err = UnMarshalProposalMessage(data)
			if err != nil {
				fmt.Printf("[NewProposal] UnMarshalProposalMassage error=%v\n", err)
				return nil
			}
			if blockProposal.From == Dereliction {
				fmt.Printf("[ValidateMod] ZoneName is not exist\n")
				return nil
			}
			f = true
			// // 查询
			// for _, value := range blockProposal.Values {
			// 	if ( value == ip) {
			// 		f = true
			// 	}
			// }
		}
		
		msg = ProposalMessage{
			Base:     base,
			Type:     Select,
			ZoneName: zoneName,
			Owner:    base.From,
			Values:   values,
			ISValidHost: f,
		}
	case Mod:
		blockProposal := new(ProposalMessage)
		if data, err := dao.Dao.GetZoneName(zoneName); err == leveldb.ErrNotFound {
			fmt.Printf("[ValidateMod] ZoneName is not exist\n")
			return nil
		} else {
			blockProposal, err = UnMarshalProposalMessage(data)
			if err != nil {
				fmt.Printf("[NewProposal] UnMarshalProposalMassage error=%v\n", err)
				return nil
			}
			if blockProposal.From == Dereliction {
				fmt.Printf("[ValidateMod] ZoneName is not exist\n")
				return nil
			}
		}
		msg = ProposalMessage{
			Base:     base,
			Type:     Mod,
			ZoneName: zoneName,
			Owner:    base.From,
			Values:   values,
		}
	default:
		fmt.Println("Unknown proposal type")
		return nil
	}
	msg.Id, err = msg.Hash()
	if err != nil {
		fmt.Printf("[NewProposal] Hash Failed err=%v\n", err)
		return nil
	}
	if conf.BCDnsConfig.Test {
		return &msg
	} else {
		err = msg.Sign()
		if err != nil {
			fmt.Printf("[NewProposal] msg.Sign error=%v\n", err)
			return nil
		}
		return &msg
	}
}

func (msg *ProposalMessage) GetPow() error {
	for {
		hash, err := msg.Hash()
		if err != nil {
			return err
		}
		//计算nonce？
		if utils.CheckProofOfWork(utils.ProposalPOW, hash) {
			break
		} else {
			msg.Nonce++
		}
	}
	return nil
}

func (msg *ProposalMessage) ValidatePow() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	if utils.CheckProofOfWork(utils.ProposalPOW, hash) {
		return true
	}
	return false
}

func (msg *ProposalMessage) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Type); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.ZoneName); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Owner); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Values); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Nonce); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *ProposalMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[ProposalMessage.Sign] generate signature failed")
}

//用于重新生成签名返回
func (msg *ProposalMessage) Sign2() ([]byte, error) {
	hash, err := msg.Hash()
	if err != nil {
		return []byte(""),err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		return sig, nil
	}
	return []byte(""), errors.New("[ProposalMessage.Sign] generate signature failed")
}


func (msg *ProposalMessage) VerifySignature() bool {
	if conf.BCDnsConfig.Test {
		return true
	} else {
		hash, err := msg.Hash()
		if err != nil {
			return false
		}
		return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
	}
}

func (msg *ProposalMessage) MarshalProposalMessage() ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}

func UnMarshalProposalMessage(data []byte) (*ProposalMessage, error) {
	msg := new(ProposalMessage)
	err := json.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

type ProposalConfirm struct {
	utils.Base
	ProposalHash []byte
	Signature    []byte
}

func NewProposalConfirm(proposalHash []byte) *ProposalConfirm {
	msg := ProposalConfirm{
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		ProposalHash: proposalHash,
	}
	if err := msg.Sign(); err != nil {
		return nil
	}
	return &msg
}

func (msg *ProposalConfirm) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.ProposalHash); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *ProposalConfirm) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[ProposalConfirm.Sign] generate signature failed")
}

func (msg *ProposalConfirm) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}

func (msg *ProposalMessage) ValidateAdd() bool {
	if !msg.VerifySignature() {
		logger.Warningf("[ValidateAdd] validate signature failed")
		return false
	}
	blockProposal := new(ProposalMessage)
	if !msg.ValidatePow() {
		logger.Warningf("[ValidateAdd] validate Pow failed")
		return false
	}
	//exist & del
	if data, err := dao.Dao.GetZoneName(msg.ZoneName); err != leveldb.ErrNotFound {
		blockProposal, err = UnMarshalProposalMessage(data)
		if err != nil {
			logger.Warningf("[ValidateAdd] UnMarshalProposalMassage error=%v", err)
			return false
		}
		if blockProposal.Owner != Dereliction {
			logger.Warningf("[ValidateAdd] ZoneName exits or get failed err=%v", err)
			return false
		}
	}
	return true //no register
}

func (msg *ProposalMessage) ValidateDel() bool {
	if !msg.VerifySignature() {
		logger.Warningf("[ValidateDel] validate signature failed")
		return false
	}
	if msg.Owner != Dereliction {
		logger.Warningf("[ValidateDel] Owner is wrong")
		return false
	}
	blockProposal := new(ProposalMessage)
	if data, err := dao.Dao.GetZoneName(msg.ZoneName); err == leveldb.ErrNotFound {
		logger.Warningf("[ValidateDel] ZoneName is not exist")
		return false
	} else {
		blockProposal, err = UnMarshalProposalMessage(data)
		if err != nil {
			logger.Warningf("[ValidateDel] UnMarshalProposalMassage error=%v", err)
			return false
		}
	}
	if msg.From != blockProposal.Owner {
		logger.Warningf("[ValidateDel] Zonename %v is not belong to %v", msg.ZoneName, msg.From)
		return false
	}
	return true
}

func (msg *ProposalMessage) ValidateMod() bool {
	if !msg.VerifySignature() {
		logger.Warningf("[ValidateMod] validate signature failed")
		return false
	}
	blockProposal := new(ProposalMessage)
	if data, err := dao.Dao.GetZoneName(msg.ZoneName); err == leveldb.ErrNotFound {
		logger.Warningf("[ValidateMod] ZoneName is not exist")
		return false
	} else {
		blockProposal, err = UnMarshalProposalMessage(data)
		if err != nil {
			logger.Warningf("[ValidateMod] UnMarshalProposalMassage error=%v", err)
			return false
		}
	}
	if msg.From != blockProposal.Owner || msg.Owner != blockProposal.Owner {
		logger.Warningf("[ValidateMod] ZoneName %v is not belong to %v", msg.ZoneName, msg.From)
		return false
	}
	return true
}

type ProposalMessages []ProposalMessage

type ProposalMessagePool struct {
	Mutex sync.Mutex
	ProposalMessages
}

func NewProposalMessagePool() ProposalMessagePool {
	pool := ProposalMessagePool{
		ProposalMessages: ProposalMessages{},
	}
	return pool
}

func (pool *ProposalMessagePool) AddProposal(p ProposalMessage) {
	pool.ProposalMessages = append(pool.ProposalMessages, p)
}

func (pool *ProposalMessagePool) Clear(index int) {
	if index == 0 {
		pool.ProposalMessages = ProposalMessages{}
	} else {
		pool.ProposalMessages = pool.ProposalMessages[index:]
	}
}

func (pool *ProposalMessagePool) Size() int {
	return len(pool.ProposalMessages)
}

func (msgs *ProposalMessages) FindByZoneName(name string) *ProposalMessage {
	for _, p := range *msgs {
		if p.ZoneName == name {
			return &p
		}
	}
	return nil
}

type BlockConfirmMessage struct {
	View int64
	utils.Base
	Id        []byte
	Proof     []byte
	Signature []byte
}

func NewBlockConfirmMessage(view int64, id []byte) (BlockConfirmMessage, error) {
	msg := BlockConfirmMessage{
		View: view,
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		Id: id,
	}
	if proof := service.CertificateAuthorityX509.Sign(id); proof != nil {
		msg.Proof = proof
	} else {
		return BlockConfirmMessage{}, errors.New("[NewBlockConfirmMessage] Get proof failed")
	}
	if err := msg.Sign(); err != nil {
		return BlockConfirmMessage{}, err
	}
	return msg, nil
}

func (msg *BlockConfirmMessage) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if jsonData, err := json.Marshal(msg.View); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	buf.Write(msg.Id)
	buf.Write(msg.Proof)
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *BlockConfirmMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[BlockConfirmMessage] Generate signature failed")
}

func (msg *BlockConfirmMessage) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}

func (msg *BlockConfirmMessage) VerifyProof() bool {
	return service.CertificateAuthorityX509.VerifySignature(msg.Proof, msg.Id, msg.From)
}

type ProposalReplyMessage struct {
	utils.Base
	Id        []byte
	Signature []byte
}

func NewProposalReplyMessage(id []byte) (*ProposalReplyMessage, error) {
	msg := &ProposalReplyMessage{
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		Id: id,
	}
	err := msg.Sign()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (msg *ProposalReplyMessage) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Id); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *ProposalReplyMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[ProposalReplyMessage] Generate signature failed")
}

func (msg *ProposalReplyMessage) VerifySignature() bool {
	if conf.BCDnsConfig.Test {
		return true
	} else {
		hash, err := msg.Hash()
		if err != nil {
			return false
		}
		return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
	}
}

type BlockCommitMessage struct {
	View int64
	utils.Base
	Id        []byte
	Proof     []byte
	Signature []byte
}

func NewBlockCommitMessage(view int64, id []byte) (BlockCommitMessage, error) {
	msg := BlockCommitMessage{
		View: view,
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		Id: id,
	}
	if proof := service.CertificateAuthorityX509.Sign(id); proof != nil {
		msg.Proof = proof
	} else {
		return BlockCommitMessage{}, errors.New("[NewBlockCommitMessage] Get proof failed")
	}
	err := msg.Sign()
	if err != nil {
		return BlockCommitMessage{}, err
	}
	return msg, nil
}

func (msg *BlockCommitMessage) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if jsonData, err := json.Marshal(msg.View); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	buf.Write(msg.Id)
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *BlockCommitMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[BlockConfirmMessage] Generate signature failed")
}

func (msg *BlockCommitMessage) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}
