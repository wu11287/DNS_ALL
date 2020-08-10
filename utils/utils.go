package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
)

func DBExists(dbFile string) bool {
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		return false
	}

	return true
}

// merge map1 into map2
func CoverMap(map1, map2 map[string]string) map[string]string {
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			map2[k] = v
		}
	}
	return map2
}

//整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int(x)
}

type ServerMsg struct {
	IsLeader bool
}

func SendStatus(isLeader bool) {
	rUdpaddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:5001")
	if err != nil {
		panic(err)
	}
	lUdpaddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:5002")
	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", lUdpaddr, rUdpaddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	msg := ServerMsg{
		IsLeader: isLeader,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
	fmt.Println("[SendStatus] msg")
}
