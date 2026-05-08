package common

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestBytesSuite(t *testing.T) {
	suite.Run(t, new(bytesSuite))
}

type bytesSuite struct {
	suite.Suite
}

func (s *bytesSuite) TestByteSize() {
	maxUint64 := ^uint64(0)
	s.Equal(ByteSize(maxUint64), "16E")

	s.Equal(ByteSize(10*EXABYTE), "10E")
	s.Equal(ByteSize(uint64(10.5*EXABYTE)), "10.5E")
	s.Equal(ByteSize(10*PETABYTE), "10P")
	s.Equal(ByteSize(uint64(10.5*PETABYTE)), "10.5P")
	s.Equal(ByteSize(10*TERABYTE), "10T")
	s.Equal(ByteSize(uint64(10.5*TERABYTE)), "10.5T")
	s.Equal(ByteSize(10*GIGABYTE), "10G")
	s.Equal(ByteSize(uint64(10.5*GIGABYTE)), "10.5G")
	s.Equal(ByteSize(100*MEGABYTE), "100M")
	s.Equal(ByteSize(uint64(100.5*MEGABYTE)), "100.5M")
	s.Equal(ByteSize(100*KILOBYTE), "100K")
	s.Equal(ByteSize(uint64(100.5*KILOBYTE)), "100.5K")
	s.Equal(ByteSize(1), "1B")
	s.Equal(ByteSize(0), "0B")
}

func (s *bytesSuite) TestToMegabytes() {
	megabytes, err := ToMegabytes("5B")
	s.NoError(err)
	s.Equal(megabytes, uint64(0))

	megabytes, err = ToMegabytes("5K")
	s.NoError(err)
	s.Equal(megabytes, uint64(0))

	megabytes, err = ToMegabytes("5M")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("5m")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("2G")
	s.NoError(err)
	s.Equal(megabytes, uint64(2*1024))

	megabytes, err = ToMegabytes("3T")
	s.NoError(err)
	s.Equal(megabytes, uint64(3*1024*1024))

	megabytes, err = ToMegabytes("4P")
	s.NoError(err)
	s.Equal(megabytes, uint64(4*1024*1024*1024))

	megabytes, err = ToMegabytes("5E")
	s.NoError(err)
	s.Equal(megabytes, uint64(5*1024*1024*1024*1024))

	megabytes, err = ToMegabytes("5MB")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("5mb")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("2GB")
	s.NoError(err)
	s.Equal(megabytes, uint64(2*1024))

	megabytes, err = ToMegabytes("3TB")
	s.NoError(err)
	s.Equal(megabytes, uint64(3*1024*1024))

	megabytes, err = ToMegabytes("4PB")
	s.NoError(err)
	s.Equal(megabytes, uint64(4*1024*1024*1024))

	megabytes, err = ToMegabytes("5EB")
	s.NoError(err)
	s.Equal(megabytes, uint64(5*1024*1024*1024*1024))

	megabytes, err = ToMegabytes("5MiB")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("5mib")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	megabytes, err = ToMegabytes("2GiB")
	s.NoError(err)
	s.Equal(megabytes, uint64(2*1024))

	megabytes, err = ToMegabytes("3TiB")
	s.NoError(err)
	s.Equal(megabytes, uint64(3*1024*1024))

	megabytes, err = ToMegabytes("4PiB")
	s.NoError(err)
	s.Equal(megabytes, uint64(4*1024*1024*1024))

	megabytes, err = ToMegabytes("5EiB")
	s.NoError(err)
	s.Equal(megabytes, uint64(5*1024*1024*1024*1024))

	_, err = ToMegabytes("5")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	_, err = ToMegabytes("5MBB")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	_, err = ToMegabytes("5BB")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	megabytes, err = ToMegabytes("\t\n\r 5MB ")
	s.NoError(err)
	s.Equal(megabytes, uint64(5))

	_, err = ToMegabytes("-5MB")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	megabytes, err = ToMegabytes("0TB")
	s.NoError(err)
	s.Equal(megabytes, uint64(0))

	megabytes, err = ToMegabytes("16247324 kB")
	s.NoError(err)
	s.Equal(megabytes, uint64(15866))

	megabytes, err = ToMegabytes("162 gB")
	s.NoError(err)
	s.Equal(megabytes, uint64(162*1024))

	megabytes, err = ToMegabytes("162 tB")
	s.NoError(err)
	s.Equal(megabytes, uint64(162*1024*1024))

	megabytes, err = ToMegabytes("162 pB")
	s.NoError(err)
	s.Equal(megabytes, uint64(162*1024*1024*1024))

	megabytes, err = ToMegabytes("2 eB")
	s.NoError(err)
	s.Equal(megabytes, uint64(2*1024*1024*1024*1024))

	megabytes, err = ToMegabytes("16247324 mB")
	s.NoError(err)
	s.Equal(megabytes, uint64(16247324))
}

func (s *bytesSuite) TestToBytes() {
	bytes, err := ToBytes("5B")
	s.NoError(err)
	s.Equal(bytes, uint64(5))

	bytes, err = ToBytes("5K")
	s.NoError(err)
	s.Equal(bytes, uint64(5*KILOBYTE))

	bytes, err = ToBytes("5M")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("5m")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("2G")
	s.NoError(err)
	s.Equal(bytes, uint64(2*GIGABYTE))

	bytes, err = ToBytes("3T")
	s.NoError(err)
	s.Equal(bytes, uint64(3*TERABYTE))

	bytes, err = ToBytes("4P")
	s.NoError(err)
	s.Equal(bytes, uint64(4*PETABYTE))

	bytes, err = ToBytes("5E")
	s.NoError(err)
	s.Equal(bytes, uint64(5*EXABYTE))

	bytes, err = ToBytes("13.5KB")
	s.NoError(err)
	s.Equal(bytes, uint64(13824))

	bytes, err = ToBytes("4.5KB")
	s.NoError(err)
	s.Equal(bytes, uint64(4608))

	bytes, err = ToBytes("5MB")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("5mb")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("2GB")
	s.NoError(err)
	s.Equal(bytes, uint64(2*GIGABYTE))

	bytes, err = ToBytes("3TB")
	s.NoError(err)
	s.Equal(bytes, uint64(3*TERABYTE))

	bytes, err = ToBytes("4PB")
	s.NoError(err)
	s.Equal(bytes, uint64(4*PETABYTE))

	bytes, err = ToBytes("5EB")
	s.NoError(err)
	s.Equal(bytes, uint64(5*EXABYTE))

	bytes, err = ToBytes("5KiB")
	s.NoError(err)
	s.Equal(bytes, uint64(5*KILOBYTE))

	bytes, err = ToBytes("5MiB")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("5mib")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	bytes, err = ToBytes("2GiB")
	s.NoError(err)
	s.Equal(bytes, uint64(2*GIGABYTE))

	bytes, err = ToBytes("3TiB")
	s.NoError(err)
	s.Equal(bytes, uint64(3*TERABYTE))

	bytes, err = ToBytes("4PiB")
	s.NoError(err)
	s.Equal(bytes, uint64(4*PETABYTE))

	bytes, err = ToBytes("5EiB")
	s.NoError(err)
	s.Equal(bytes, uint64(5*EXABYTE))

	_, err = ToBytes("5")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	_, err = ToBytes("5MBB")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	_, err = ToBytes("5BB")
	s.Error(err)

	bytes, err = ToBytes("\t\n\r 5MB ")
	s.NoError(err)
	s.Equal(bytes, uint64(5*MEGABYTE))

	_, err = ToBytes("-5MB")
	s.Error(err)
	s.Containsf(err.Error(), "unit of measurement", "error message")

	bytes, err = ToBytes("0TB")
	s.NoError(err)
	s.Equal(bytes, uint64(0))
}
