# /usr/bin/python3

from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread, Lock

hostList = []
lock = Lock()

port = 5000

def tryConnect(ip, n):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.settimeout(2)

    try:
        sock.connect((ip, port))
        print('find %s in network', ip)
        host = ip + ' ' + 's' + str(n)
        lock.acquire()
        hostList.append(host)
        lock.release()

    except Exception:
        pass

    sock.close()


def getHostList():
    ip_prefix = '10.50.87.'

    tlist = []

    for i in range(1, 254):
        ip = ip_prefix + str(i)
        t = Thread(target = tryConnect, args = (ip, i))
        tlist.append(t)
        t.start()

    for t in tlist:
        t.join()



def saveHostList():
    with open('/tmp/hosts', 'w+') as f:
        for host in hostList:
            f.write(host + '\n')


def main():
    getHostList()
    saveHostList()

main()
