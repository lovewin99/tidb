package encrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"strings"
)

func HexStringToByte(s string) []byte {
	ts := []byte(s)
	l := (strings.Count(s, "") - 1) / 2
	baKeyWord := make([]byte, l)
	for i := 0; i < l; i++ {
		t := make([]byte, 1)
		hex.Decode(t, ts[i*2:i*2+2])
		baKeyWord[i] = t[0]
	}
	return baKeyWord
}

//解密数据
func Decrypt(src, key, iv []byte) (data []byte, err error) {
	decrypted := make([]byte, len(src))
	var aesBlockDecrypter cipher.Block
	aesBlockDecrypter, err = aes.NewCipher(key)
	if err != nil {
		println(err.Error())
		return nil, err
	}
	defer func() {
		if err1 := recover(); err1 != nil {
			data = nil
			err = errors.New(err1.(string))
		}
	}()
	aesDecrypter := cipher.NewCBCDecrypter(aesBlockDecrypter, iv)
	aesDecrypter.CryptBlocks(decrypted, src)
	data, err = PKCS5Trimming(decrypted)
	return
}

/**
解包装
*/
func PKCS5Trimming(encrypt []byte) ([]byte, error) {
	padding := encrypt[len(encrypt)-1]
	if len(encrypt) >= int(padding) {
		return encrypt[:len(encrypt)-int(padding)], nil
	} else {
		return []byte{}, errors.New("interface conversion: interface {} is runtime.errorString, not string")
	}
}

/**
PKCS5包装
*/
func PKCS5Padding(cipherText []byte, blockSize int) []byte {
	padding := blockSize - len(cipherText)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(cipherText, padText...)
}

func ByteToHexString(b []byte) string {
	var s []string
	for i := 0; i < len(b); i++ {
		str := hex.EncodeToString([]byte{b[i]})
		if len(str) > 3 {
			s = append(s, string([]byte(str)[6:]))
		} else if len(str) < 2 {
			s = append(append(s, "0"), str)
		} else {
			s = append(s, str)
		}
	}
	return strings.Join(s, "")
}

//加密数据
func Encrypt(data, key, iv []byte) (rd []byte, err error) {
	aesBlockEncrypter, err := aes.NewCipher(key)
	content := PKCS5Padding(data, aesBlockEncrypter.BlockSize())
	encrypted := make([]byte, len(content))
	if err != nil {
		println(err.Error())
		return nil, err
	}
	defer func() {
		if err1 := recover(); err1 != nil {
			rd = nil
			err = errors.New(err1.(string))
		}
	}()
	aesEncrypter := cipher.NewCBCEncrypter(aesBlockEncrypter, iv)
	aesEncrypter.CryptBlocks(encrypted, content)
	return encrypted, nil
}
