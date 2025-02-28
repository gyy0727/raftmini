//*声明包名为wal（预写式日志模块）
package wal

//*导入Prometheus客户端库
import "github.com/prometheus/client_golang/prometheus"

//*包级变量定义（指标对象）
var (
    syncDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
        Namespace: "etcd",          //*指标命名空间（通常对应系统名称）
        Subsystem: "disk",          //*子系统分类（磁盘相关操作）
        Name:      "wal_fsync_duration_seconds", //*指标名称（WAL的fsync持续时间）
        Help:      "The latency distributions of fsync called by wal.", //*帮助说明
        Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), //*自定义指数型桶配置
    })
)

//*包初始化函数
func init() {
    prometheus.MustRegister(syncDurations) //*强制注册指标到默认注册表
}
