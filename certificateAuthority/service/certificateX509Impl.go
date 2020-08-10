package service

import (
	"BCDns_0.1/bcDns/conf"
	"BCDns_0.1/utils"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

var (
	LocalPrivateName         = "LocalPrivate.pem"
	RootCertificateName      = "RootCertificate.cer"
	LocalCertificateName     = "LocalCertificate.cer"
	CertificatesPath         = "../conf/"
	CertificateAuthorityX509 *CAX509
)

func init() {
	if val, ok := os.LookupEnv("LocalPrivateName"); ok {
		LocalPrivateName = val
	}
	if val, ok := os.LookupEnv("RootCertificateName"); ok {
		RootCertificateName = val
	}
	if val, ok := os.LookupEnv("LocalCertificateName"); ok {
		LocalCertificateName = val
	}
	if val, ok := os.LookupEnv("CertificatesPath"); ok {
		CertificatesPath = val
	}
}

type CheckSigFailedErr struct {
	Msg string
}

type Node struct {
	Cert   x509.Certificate
	Member interface{}
}

func (err CheckSigFailedErr) Error() string {
	return err.Msg
}

type CAX509 struct {
	Mutex             sync.Mutex
	Certificates      map[string]x509.Certificate
	CertificatesOrder []*x509.Certificate
	NodeId            int64
	PrivateKey        *rsa.PrivateKey
}

func init() {
	certs, certsOrder := make(map[string]x509.Certificate), make([]*x509.Certificate, 0)
	dir, err := ioutil.ReadDir(CertificatesPath)
	if err != nil {
		log.Fatal(err)
	}
	for _, fileInfo := range dir {
		fileName := fileInfo.Name()
		ok, err := regexp.MatchString(`.*\.cer$`, fileName)
		if err != nil {
			log.Fatal(err)
		}
		if strings.Compare(fileName, RootCertificateName) == 0 {
			continue
		}
		if ok {
			cert := loadCertificate2(CertificatesPath + fileName)
			if cert == nil {
				os.Exit(-1)
			}
			name, err := utils.GetCertId(*cert)
			if err != nil {
				fmt.Printf("[Load certificates] error=%v\n", err)
				panic(err)
			}
			certs[name] = *cert
			certsOrder = insertCertificateByOrder(certsOrder, cert)
		}
	}
	nodeid := int64(-1)
	for i, c := range certsOrder {
		id, err := utils.GetCertId(*c)
		if err != nil {
			fmt.Printf("[Load certificates] error=%v\n", err)
			continue
		}
		if id == conf.BCDnsConfig.HostName {
			nodeid = int64(i)
			break
		}
	}
	if nodeid == -1 {
		fmt.Printf("[Load certificates]\n")
		panic("Can not find local certificate")
	}
	pKey := loadPrivateKey2()
	if pKey == nil {
		panic("[Load certificates] loadPrivateKey2 failed")
	}
	CertificateAuthorityX509 = &CAX509{
		Mutex:             sync.Mutex{},
		Certificates:      certs,
		CertificatesOrder: certsOrder,
		NodeId:            nodeid,
		PrivateKey:        pKey,
	}
}

func (ca *CAX509) Sign(msg []byte) []byte {
	if digest, err := getDigest2(msg); err != nil {
		fmt.Printf("[CAX509.Sign] getDigest2 error=%v\n", err)
		return nil
	} else {
		if signature, err := rsa.SignPKCS1v15(rand.Reader, ca.PrivateKey, crypto.SHA256, digest[:]); err != nil {
			fmt.Printf("[CAX509.Sign] SignPKCS1v15 error=%v\n", err)
			return nil
		} else {
			return signature
		}
	}
}

func (ca *CAX509) VerifySignature(sig, msg []byte, Id string) bool {
	if cert, ok := ca.Certificates[Id]; ok {
		publicKey := cert.PublicKey.(*rsa.PublicKey)
		if digest, err := getDigest2(msg); err != nil {
			fmt.Println(err)
		} else {
			if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, digest, sig); err == nil {
				return true
			}
		}
	}
	return false
}

func (*CAX509) Encode(msg []byte, Id string) []byte {
	panic("implement me")
}

func (*CAX509) Decode(EncodeMsg []byte, Id string) []byte {
	panic("implement me")
}

func (ca *CAX509) GetCerts() map[string]x509.Certificate {
	return ca.Certificates
}

func (ca *CAX509) DelCert(Id string) error {
	if _, ok := ca.Certificates[Id]; ok {
		ca.Mutex.Lock()
		delete(ca.Certificates, Id)
		ca.Mutex.Unlock()
	}
	filename := Id + ".crt"
	_, err := os.Stat(filename)
	if err == nil {
		err = os.Remove(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ca *CAX509) GetSeeds() []string {
	var seeds []string
	for _, cert := range ca.Certificates {
		for _, ip := range cert.IPAddresses {
			seeds = append(seeds, strings.Join([]string{ip.String(), conf.BCDnsConfig.Port}, ":"))
		}
	}
	return seeds
}

func (ca *CAX509) VerifyCertificate(data []byte) bool {
	cert, err := x509.ParseCertificate(data)
	if err != nil {
		fmt.Println("Verify: parse failed", err)
		return false
	}
	rootCert := loadCertificate2(CertificatesPath + RootCertificateName)
	if rootCert == nil {
		return false
	}
	if err := cert.CheckSignatureFrom(rootCert); err != nil {
		fmt.Println("Verify failed", err)
		return false
	}
	return true
}

func (ca *CAX509) GetLocalCertificate() (*x509.Certificate, []byte) {
	return loadCertificate2(CertificatesPath + LocalCertificateName), loadCertificate2Bytes(CertificatesPath + LocalCertificateName)
}

func (ca *CAX509) GetNetworkSize() int {
	return len(ca.Certificates)
}

func (ca *CAX509) GetF() int {
	return (ca.GetNetworkSize() - 1) / 3
}

func (ca *CAX509) Exits(id string) bool {
	_, ok := ca.Certificates[id]
	return ok
}

func (ca *CAX509) Check(n int) bool {
	ca.Mutex.Lock()
	defer ca.Mutex.Unlock()

	return n >= ca.GetF()*2+1
}

func (ca *CAX509) IsLeaderNode(id int64) bool {
	ca.Mutex.Lock()
	defer ca.Mutex.Unlock()
	return ca.NodeId == id
}

func loadPrivateKey2() *rsa.PrivateKey {
	fileInfo, err := os.Stat(CertificatesPath + LocalPrivateName)
	if err != nil {
		fmt.Printf("[loadPrivateKey2] os.Stat error=%v\n", err)
		return nil
	}

	content := make([]byte, fileInfo.Size())
	if file, err := os.Open(CertificatesPath + LocalPrivateName); err != nil {
		fmt.Printf("[loadPrivateKey2] os.Open error=%v\n", err)
		return nil
	} else {
		_, err := file.Read(content)
		if err != nil {
			fmt.Printf("[loadPrivateKey2] os.Read error=%v\n", err)
			return nil
		}
		block, _ := pem.Decode(content)
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			fmt.Printf("[loadPrivateKey2] ParsePKCS1PrivateKey error=%v\n", err)
			return nil
		}
		return key
	}
}

func loadCertificate2(fileName string) *x509.Certificate {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	content := make([]byte, fileInfo.Size())
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	_, err = file.Read(content)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	block, _ := pem.Decode(content)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return cert
}

func loadCertificate2Bytes(fileName string) []byte {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	content := make([]byte, fileInfo.Size())
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	_, err = file.Read(content)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	block, _ := pem.Decode(content)
	return block.Bytes
}

func getDigest2(msg []byte) ([]byte, error) {
	hash := crypto.SHA256.New()
	if _, err := hash.Write(msg); err != nil {
		return []byte{}, nil
	}
	digest := hash.Sum(nil)
	return digest, nil
}

func insertCertificateByOrder(certs []*x509.Certificate, cert *x509.Certificate) []*x509.Certificate {
	for i, c := range certs {
		if c.SerialNumber.Cmp(cert.SerialNumber) > 0 {
			certs = append(certs[:i+1], certs[i:]...)
			certs[i] = cert
			return certs
		} else if c.SerialNumber.Cmp(cert.SerialNumber) == 0 {
			panic("[Cert] Wrong SerialNumber")
		}
	}
	return append(certs, cert)
}
