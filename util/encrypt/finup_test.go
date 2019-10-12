package encrypt

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testFinupSuite{})

type testFinupSuite struct {
}

func (s *testFinupSuite) TestHexStringToByte(c *C) {
	defer testleak.AfterTest(c)()

	str := "cde5fdc103019fe91219087a983998f2"
	b := []byte{205, 229, 253, 193, 3, 1, 159, 233, 18, 25, 8, 122, 152, 57, 152, 242}

	c.Assert(string(HexStringToByte(str)), Equals, string(b))

}

func (s *testFinupSuite) TestByteToHexString(c *C) {
	defer testleak.AfterTest(c)()

	str := "cde5fdc103019fe91219087a983998f2"
	b := []byte{205, 229, 253, 193, 3, 1, 159, 233, 18, 25, 8, 122, 152, 57, 152, 242}

	c.Assert(ByteToHexString(b), Equals, str)

}

func (s *testFinupSuite) TestEncrypt(c *C) {
	defer testleak.AfterTest(c)()

	str := "abc123456^&"
	b := []byte{193, 48, 138, 157, 143, 116, 233, 236, 66, 100, 218, 10, 3, 236, 137, 12}
	key := "fc07d382b5475798"
	iv := "b5475798f71dc641"

	d, err := Encrypt([]byte(str), []byte(key), []byte(iv))

	c.Assert(err, IsNil)
	c.Assert(string(d), Equals, string(b))

}

func (s *testFinupSuite) TestDecrypt(c *C) {
	defer testleak.AfterTest(c)()

	str := "abc123456^&"
	b := []byte{193, 48, 138, 157, 143, 116, 233, 236, 66, 100, 218, 10, 3, 236, 137, 12}
	key := "fc07d382b5475798"
	iv := "b5475798f71dc641"

	d, err := Decrypt(b, []byte(key), []byte(iv))

	c.Assert(err, IsNil)

	c.Assert(string(d), Equals, str)

}
