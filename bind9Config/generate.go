package bind9Config

import (
	"BCDns_0.1/bcDns/conf"
	"BCDns_0.1/blockChain"
	"BCDns_0.1/messages"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"io"
	"time"
)

const (
	BindConfigFile = "./db.root"
	NameComment    = ";; "
)

var Generator *ConfigGenerator

type ConfigGenerator struct{}

func init() {
	Generator = new(ConfigGenerator)
}

func (g *ConfigGenerator) Run() {
	for {
		select {
		case <-time.After(conf.BCDnsConfig.ConfigInterval):
			generateConfig()
		}
	}
}

func generateConfig() {
	f, err := os.OpenFile(BindConfigFile, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("[generateConfig] error=%v\n", err)
		return
	}
	defer f.Close()
	config, err := blockChain.ZoneStatePool.GetModifiedData()
	if err != nil {
		fmt.Printf("[generateConfig] error=%v\n", err)
		return
	}

	reader, output, replace := bufio.NewReader(f), make([]byte, 0), false
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[generateConfig] error=%v\n", err)
			}
			return
		}
		if bytes.HasPrefix(line, []byte(NameComment)) {
			infos := bytes.Split(line, []byte(" "))
			if rrs, ok := config[string(infos[1])]; ok && rrs.Owner != messages.Dereliction && !replace {
				replace = true
				output = append(output, generate(string(infos[1]), rrs)...)
			} else if replace {
				replace = false
			}
		} else if !replace {
			output = append(output, line...)
		}
	}
	f.Write(output)
}

func generate(name string, rrs blockChain.ZoneRecord) []byte {
	output := make([]byte, 0)
	output = append(output, []byte(NameComment+name+"\n")...)
	for _, rr := range rrs.Values {
		output = append(output, []byte(rr+"\n")...)
	}
	output = append(output, []byte(NameComment+name+"\n")...)
	return output
}
