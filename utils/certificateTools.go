package utils

import (
	"crypto/x509"
	"fmt"
	"reflect"
)

var (
	x509Certificate    = reflect.TypeOf(x509.Certificate{})
	x509CertificatePtr = reflect.TypeOf(&x509.Certificate{})
)

type GetIDError struct {
	Msg string
}

func (err GetIDError) Error() string {
	return err.Msg
}

func GetCertId(cert interface{}) (string, error) {
	switch reflect.TypeOf(cert) {
	case x509CertificatePtr:
		certificate := cert.(*x509.Certificate)
		return certificate.Subject.CommonName, nil
	case x509Certificate:
		certificate := cert.(x509.Certificate)
		return certificate.Subject.CommonName, nil
	default:
		fmt.Println("Type is ", reflect.TypeOf(cert))
		return "", GetIDError{"Unknown certificate type"}
	}
}
