package blockChain

import (
	"BCDns_0.1/bcDns/conf"
	"BCDns_0.1/dao"
	"fmt"
	"strings"
)

var ZoneStatePool *ZoneStatePoolT

type ZoneStatePoolT struct {
	*dao.DB
}

type ZoneRecord struct {
	Owner  string
	Values []string
}

func init() {
	var err error
	ZoneStatePool, err = NewZoneStatePool()
	if err != nil {
		fmt.Printf("[ZoneStatePool error=%v]", err)
		panic(err)
	}
}

func NewZoneStatePool() (*ZoneStatePoolT, error) {
	db, err := dao.NewDB(strings.Join([]string{conf.BCDnsConfig.HostName, "zoneState"}, "_"))
	if err != nil {
		return nil, err
	}
	return &ZoneStatePoolT{
		DB: db,
	}, nil
}

func (z *ZoneStatePoolT) Modify(zoneName string) {
	_ = z.DB.Set([]byte(zoneName), []byte{})
}

func (z *ZoneStatePoolT) GetModifiedData() (map[string]ZoneRecord, error) {
	iter := z.DB.NewIterator(nil, nil)
	result := make(map[string]ZoneRecord)
	for iter.Next() {
		zoneName := string(iter.Key())
		record, err := BlockChain.FindDomain(zoneName)//record是一个ProposalMessage
		if err != nil {
			fmt.Println("[ZoneStatePool] BlockChain.FindDomain error", err)
			continue
		}
		result[zoneName] = ZoneRecord{
			Owner:  record.Owner,
			Values: record.Values,//表示RRs
		}
		_ = z.DB.Delete(iter.Key(), nil)
	}
	return result, nil
}
