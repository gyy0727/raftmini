package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	raft "github.com/gyy0727/raftmini"
	pioutil "github.com/gyy0727/raftmini/pkg/ioutil"
	"github.com/gyy0727/raftmini/pkg/pbutil"
	"github.com/gyy0727/raftmini/pkg/snap/snappb"
	"github.com/gyy0727/raftmini/raftpb"
	"go.uber.org/zap"
)

// *后缀
const (
	snapSuffix = ".snap"
)

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot") //*没有可用的快照
	ErrEmptySnapshot = errors.New("snap: empty snapshot")        //*快照为空
	ErrCRCMismatch   = errors.New("snap: crc mismatch")          //*crc校验失败
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	//*快照目录中允许存在的文件列表
	validFiles = map[string]bool{
		"db": true,
	}
)

// *用于管理快照文件
type Snapshotter struct {
	dir string //*快照文件的存储目录
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

// *用于保存 Raft 快照
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

// *用于将 Raft 快照（raftpb.Snapshot）保存到磁盘
func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	//*记录开始时间
	start := time.Now()
	//*生成快照文件名
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	//*序列化快照
	b := pbutil.MustMarshal(snapshot)
	//*计算crc校验和
	crc := crc32.Update(0, crcTable, b)
	//*构造快照
	snap := snappb.Snapshot{Crc: crc, Data: b}
	//*序列化封装好的快照
	d, err := snap.Marshal()
	if err != nil {
		return err
	} else {
		marshallingDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	}
	//*写入文件
	err = pioutil.WriteAndSyncFile(path.Join(s.dir, fname), d, 0666)
	if err == nil {
		saveDurations.Observe(float64(time.Since(start)) / float64(time.Second))
	} else {
		err1 := os.Remove(path.Join(s.dir, fname))
		if err1 != nil {
			logger.Error("failed to remove broken snapshot file",
				zap.String("file", path.Join(s.dir, fname)),
			)

		}
	}
	return err
}

func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := path.Join(dir, name)
	snap, err := Read(fpath)
	if err != nil {
		renameBroken(fpath)
	}
	return snap, err
}

// *用于从文件中读取并解析 Raft 快照
func Read(snapname string) (*raftpb.Snapshot, error) {
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		logger.Error("cannot read file",
			zap.String("file", snapname),
			zap.Error(err),
		)
		return nil, err
	}

	if len(b) == 0 {
		logger.Error("unexpected empty snapshot",
			zap.String("file", snapname),
		)
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		logger.Error("corrupted snapshot file",
			zap.String("file", snapname),
			zap.Error(err),
		)
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		logger.Error("unexpected empty snapshot",
			zap.String("file", snapname),
		)
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		logger.Error("corrupted snapshot file: crc mismatch",
			zap.String("file", snapname),
		)
		return nil, ErrCRCMismatch
	}

	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		logger.Error("corrupted snapshot file",
			zap.String("file", snapname),
			zap.Error(err),
		)
		return nil, err
	}
	return &snap, nil
}

// *获取快照目录下包含合法后缀的所有文件名
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	//*获取目录下的所有文件名
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	//*检查文件名的后缀
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	//*返回符合的排序后的文件名
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

// *用于过滤文件名列表，只保留符合快照文件后缀的文件
// *并对非快照文件进行验证和警告
func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			//*如果文件名不在 validFiles 中，记录警告日志
			if _, ok := validFiles[names[i]]; !ok {
				logger.Warn("skipped unexpected non snapshot file",
					zap.String("file", names[i]),
				)

			}
		}
	}
	return snaps
}

// *用于将损坏的快照文件重命名为 .broken 后缀的文件，
// *并在重命名失败时记录警告日志
func renameBroken(path string) {
	brokenPath := path + ".broken"
	if err := os.Rename(path, brokenPath); err != nil {
		logger.Warn("cannot rename broken snapshot file",
			zap.String("original_path", path),
			zap.String("broken_path", brokenPath),
			zap.Error(err),
		)

	}
}
