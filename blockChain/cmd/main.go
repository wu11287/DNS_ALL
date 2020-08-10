package main

import (
	"BCDns_0.1/blockChain"
	"BCDns_0.1/messages"
	"encoding/json"
	"fmt"
	"runtime"
)

func main() {
	for i := 0; i < 100000; i++ {
		p := messages.NewProposal(".com", messages.Add, map[string]string{})
		r := messages.NewProposalAuditResponse(*p)
		ap, err := messages.NewAuditedProposal(*p, messages.ProposalAuditResponses{
			"1": *r,
		}, 0)
		if err != nil {
			panic(err)
		}
		b := blockChain.NewBlock(messages.AuditedProposalSlice{
			*ap,
		}, []byte("test"), 0, true)
		m, err := blockChain.NewBlockMessage(b)
		if err != nil {
			panic(err)
		}
		runtime.GC()
		_, err = json.Marshal(*m)
		if err != nil {
			fmt.Printf("[LeaderNode] CurrentBlock marshal failed err=%v\n", err)
			continue
		}
	}
}
