package main
import (
    "fmt"
    "net"
    //"strings"
    "time"
)

func main() {
    //组织一个udp地址结构，指定服务器的ip+port
    srvAddr, err := net.ResolveUDPAddr("udp","127.0.0.1:8888")
    if err != nil {
        fmt.Println("net.ResolveUDPAddr Err",err)
        return
    }
    //defer listener.Close()
    fmt.Println("udp服务器创建ok")
    //2、阻塞监听客户端连接请求，成功建立连接，返回用于通信的socket
    udpConn, err := net.ListenUDP("udp", srvAddr)
    if err != nil {
        fmt.Println("listener.Accept() Err",err)
        return
    }
    defer udpConn.Close()
    fmt.Println("udp服务器socket创建完成！")

    //3、读取客户端发送的数据
    buf := make([]byte,4096)
    n, clientAddr, err := udpConn.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("udp read Err",err)
        return
    }  
    fmt.Println("服务器读到的数据，",string(buf[:n]))
    nowTime := time.Now().String()
    _, err = udpConn.WriteToUDP([]byte(nowTime),clientAddr)
    if err != nil {
        fmt.Println("WriteToUDP Err",err)
        return
    }
    // string_slice := strings.Split(string(buf[:n]),"*")
    // host:=string_slice[0]
    // ip := string_slice[1]
    
    // fmt.Println("HOST: ",host)
    // fmt.Println("IP: ", ip)
    
    // res := "true"
    // udpConn.WriteToUDP([]byte(res), clientAddr)//读多少，写多少
}