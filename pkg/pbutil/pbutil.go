package pbutil

import "github.com/coreos/pkg/capnslog"

var (
	plog = capnslog.NewPackageLogger("github.com/coreos/etcd/pkg", "flags")
)

// *定义接口：可序列化对象接口（类似protobuf的Marshal接口）
type Marshaler interface {
	Marshal() (data []byte, err error)
}

// *定义接口：可反序列化对象接口（类似protobuf的Unmarshal接口）
type Unmarshaler interface {
	Unmarshal(data []byte) error
}

// *强制序列化函数：失败时触发panic
func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		plog.Panicf("marshal should never fail (%v)", err)
	}
	return d
}

// *强制反序列化函数：失败时触发panic
func MustUnmarshal(um Unmarshaler, data []byte) {
	if err := um.Unmarshal(data); err != nil {
		plog.Panicf("unmarshal should never fail (%v)", err)
	}
}

// *尝试反序列化函数：返回是否成功
func MaybeUnmarshal(um Unmarshaler, data []byte) bool {
	if err := um.Unmarshal(data); err != nil {
		return false
	}
	return true
}

// *安全获取布尔指针值：返回（值，是否存在）
func GetBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

// *创建布尔指针的便捷函数
func Boolp(b bool) *bool { return &b }
