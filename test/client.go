package main

import (
    "fmt"
    "net"
)

func main(){
    //指定服务器IP + port创建通信套接字
    conn, err := net.Dial("udp","127.0.0.1:8888")
    if err != nil {
        fmt.Println("net.Dial err",err)
        return
    }
    defer conn.Close()
    //主动写数据给服务器
    _, err = conn.Write([]byte("hrlll"))
    if err != nil {
        fmt.Println("conn.Write err",err)
        return
    }
    //接收服务器回发数据
    buf := make([]byte,4096)
    n, err := conn.Read(buf)
    if err != nil {
        fmt.Println("conn.Write err",err)
        return
    }
    fmt.Println("服务器回发",string(buf[:n]))
}