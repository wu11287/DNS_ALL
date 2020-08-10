package model

import (
	"BCDns_0.1/bcDns/conf"
	"BCDns_0.1/blockChain"
	"BCDns_0.1/certificateAuthority/service"
	"BCDns_0.1/messages"
	"BCDns_0.1/utils"
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

type BlockMessage struct {
	View int64
	utils.Base
	blockChain.Block
	AbandonedProposal messages.ProposalMessages
	Signature         []byte
}

//TODO
func NewBlockMessage(view int64, b *blockChain.Block, abandonedP messages.ProposalMessages) (BlockMessage, error) {
	msg := BlockMessage{
		View: view,
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		Block:             *b,
		AbandonedProposal: abandonedP,
	}
	err := msg.Sign()
	if err != nil {
		return BlockMessage{}, err
	}
	return msg, nil
}

func (msg *BlockMessage) Hash() ([]byte, error) {
	buf := bytes.Buffer{}
	if hash, err := msg.Block.Hash(); err != nil {
		return nil, err
	} else {
		buf.Write(hash)
	}
	if jsonData, err := json.Marshal(msg.Base); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if jsonData, err := json.Marshal(msg.AbandonedProposal); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *BlockMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[BlockMessage.Sign] generate signature failed")
}

func (msg *BlockMessage) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}

type ViewChangeMessage struct {
	View int64
	utils.Base
	BlockHeader blockChain.BlockHeader
	Proofs      map[string][]byte
	Block       BlockMessage
	Signature   []byte
}

func NewViewChangeMessage(lastBlock *blockChain.BlockValidated, view int64, b *BlockMessage) (ViewChangeMessage, error) {
	msg := ViewChangeMessage{
		View: view,
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		BlockHeader: lastBlock.BlockHeader,
		Proofs:      lastBlock.Signatures,
		Block:       *b,
	}
	err := msg.Sign()
	if err != nil {
		return ViewChangeMessage{}, err
	}
	return msg, nil
}

func (msg *ViewChangeMessage) Hash() ([]byte, error) {
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
	if hash, err := msg.BlockHeader.MarshalBlockHeader(); err != nil {
		return nil, err
	} else {
		buf.Write(hash)
	}
	if jsonData, err := json.Marshal(msg.Proofs); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if hash, err := msg.Block.Hash(); err != nil {
		return nil, err
	} else {
		buf.Write(hash)
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *ViewChangeMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[BlockMessage.Sign] generate signature failed")
}

func (msg *ViewChangeMessage) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}

func (msg *ViewChangeMessage) VerifySignatures() bool {
	headerHash, err := msg.BlockHeader.MarshalBlockHeader()
	if err != nil {
		return false
	}
	hash := utils.SHA256(headerHash)
	for host, sig := range msg.Proofs {
		if !service.CertificateAuthorityX509.VerifySignature(sig, hash, host) {
			return false
		}
	}
	return true
}

type NewViewMessage struct {
	View int64
	utils.Base
	Height    uint
	BlockMsg  BlockMessage
	Proofs    map[string][]byte
	V         map[string]ViewChangeMessage
	Signature []byte
}

func NewNewViewMessage(view int64, viewChangeMsgs map[string]ViewChangeMessage, block BlockMessage, proofs map[string][]byte) (NewViewMessage, error) {
	msg := NewViewMessage{
		View: view,
		Base: utils.Base{
			From:      conf.BCDnsConfig.HostName,
			TimeStamp: time.Now().Unix(),
		},
		Height:   block.Height,
		BlockMsg: block,
		Proofs:   proofs,
		V:        viewChangeMsgs,
	}
	err := msg.Sign()
	if err != nil {
		return NewViewMessage{}, err
	}
	return msg, nil
}

func (msg *NewViewMessage) Hash() ([]byte, error) {
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
	if jsonData, err := json.Marshal(msg.Height); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	if hash, err := msg.BlockMsg.Hash(); err != nil {
		return nil, err
	} else {
		buf.Write(hash)
	}
	if jsonData, err := json.Marshal(msg.Proofs); err != nil {
		return nil, err
	} else {
		buf.Write(jsonData)
	}
	for _, v := range msg.V {
		if hash, err := v.Hash(); err != nil {
			return nil, err
		} else {
			buf.Write(hash)
		}
	}
	return utils.SHA256(buf.Bytes()), nil
}

func (msg *NewViewMessage) Sign() error {
	hash, err := msg.Hash()
	if err != nil {
		return err
	}
	if sig := service.CertificateAuthorityX509.Sign(hash); sig != nil {
		msg.Signature = sig
		return nil
	}
	return errors.New("[BlockMessage.Sign] generate signature failed")
}

func (msg *NewViewMessage) VerifySignature() bool {
	hash, err := msg.Hash()
	if err != nil {
		return false
	}
	return service.CertificateAuthorityX509.VerifySignature(msg.Signature, hash, msg.From)
}
