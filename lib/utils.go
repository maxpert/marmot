package lib

import (
	"encoding/binary"
	"io"
)

func writeUint32(writer io.Writer, val uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, val)
	_, err := writer.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func readUint32(reader io.Reader) (uint32, error) {
	buff := make([]byte, 4, 4)
	_, err := io.LimitReader(reader, 4).Read(buff)
	if err != nil {
		return 0, err
	}

	val := binary.BigEndian.Uint32(buff)
	return val, nil
}
