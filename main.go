package main

import (
	"encoding/json"
	"flag"
	"net"
	"strconv"
	"time"
)

type OperationType uint8

const (
	Add OperationType = iota
	Del
	Mod
)

type Order struct {
	OptType  OperationType
	ZoneName string
	Values   map[string]string
}

var (
	mode = flag.Uint("mode", uint(Mod), "Run mod")
	frequency = flag.Int("frq", 20, "frequency")
)

func main() {
	flag.Parse()
	rUdpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8888")
	if err != nil {
		panic(err)
	}
	lUdpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8887")
	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", lUdpAddr, rUdpAddr)
	if err != nil {
		panic(err)
	}
	i := 0
	switch *mode {
	case uint(0):
		msg := Order{
			OptType:Add,
			ZoneName:"com.",
			Values: map[string]string{
				"A":strconv.Itoa(i),
			},
		}
		jsonData, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
		conn.Write(jsonData)
	case uint(1):
		du := time.Duration(*frequency) * time.Millisecond
		for count := 65 * 1000 / *frequency; count > 0; count-- {
			msg := Order{
				OptType:Mod,
				ZoneName:"com.",
				Values: map[string]string{
					"A":strconv.Itoa(i),
				},
			}
			jsonData, err := json.Marshal(msg)
			if err != nil {
				panic(err)
			}
			conn.Write(jsonData)
			i++
			time.Sleep(du)
		}
	case uint(2):
		msg := Order{
			OptType:Del,
			ZoneName:"com.",
		}
		jsonData, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
		conn.Write(jsonData)
	}
}