package walpb
import "errors"

var (
	ErrCRCMismatch = errors.New("walpb: crc mismatch")

)

//*检查两个crc值是否相等
func (rec *Record) Validate(crc uint32)error{
	if rec.Crc ==crc{
		return nil
	}
	return ErrCRCMismatch
}