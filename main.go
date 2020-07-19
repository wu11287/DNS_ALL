package main

import (
	"encoding/json"
	"flag"
    "fmt"
	"net"
	"os/exec"
	"strconv"
	"time"
)

var (
	ProjectPath = "/go/src/BCDns_0.1/"
)

type OperationType uint8

const (
	Add OperationType = iota
	Del
	Mod
	Select
)



type Order struct {
	OptType  OperationType
	ZoneName string
	Values   []string
}

var (
	mode = flag.Uint("mode", uint(Mod), "Run mod")
	frequency = flag.Float64("frq", 20, "frequency ms")
)

func main() {
	flag.Parse()
	rUdpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8888")
	if err != nil {
		panic(err)
	}
	fmt.Println("udp 地址结构创建完成")

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
			Values: []string{
				strconv.Itoa(i),
			},
		}
		jsonData, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
        a := 0
		a , err = conn.Write(jsonData)
		fmt.Println(a, err)
	case uint(1):
		du := time.Duration(*frequency * 1000) * time.Microsecond
		for count := 65 * 1000 / *frequency; count > 0; count-- {
			msg := Order{
				OptType:Mod,
				ZoneName:"com.",
				Values: []string{
					strconv.Itoa(i),
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
		cmd := exec.Command(ProjectPath + "stop.sh")
		err = cmd.Run()
		if err != nil {
			panic(err)
		}
	case uint(3):
		fmt.Println("uint 3")
		msg := Order{
			OptType:Select,
			ZoneName:"com.",
			Values: []string{
				"172.182.18.1",
			}, 
		}
		jsonData, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
        // a := 0
		_ , err = conn.Write(jsonData)
		//receive
		buf := make([]byte,4096)
		fmt.Println("11111")
		n, err := conn.Read(buf)
		fmt.Println("fffff")
    	if err != nil {
        	fmt.Println("conn.Write err",err)
        	return
    	}
    	fmt.Println("服务器回发",string(buf[:n]))
		
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

	fmt.Println("end")
}