package walpb
import "errors"

var (
	ErrCRCMismatch = errors.New("walpb: crc mismatch")

)

//*检查record记录的crc值和传入的是否相等,不相等就返回错误
func (rec *Record) Validate(crc uint32)error{
	if rec.Crc ==crc{
		return nil
	}
	return ErrCRCMismatch
}