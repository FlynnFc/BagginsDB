package database

import (
	"fmt"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"
)

type SSTableManager struct {
	dir               string
	CompactionManager *CompactionManager
	threshhold        int
	sparseInterval    int
	fpRate            float32
	logger            *zap.Logger
}

func (sm *SSTableManager) LoadCurrentSSTables() [][]*SSTable {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		sm.logger.Error("Failed to read SSTable dir", zap.Error(err))
	}

	sstables := [][]*SSTable{}
	sstables = append(sstables, []*SSTable{})
	level := 0
	for {
		anyseen := false
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), fmt.Sprintf("sst_%d", level)) {
				anyseen = true
				sst, err := LoadSSTable(path.Join(sm.dir, e.Name()))
				if err != nil {
					sm.logger.Error("Failed to load SSTable", zap.Error(err))
				}
				if len(sstables) < level+1 {
					sstables = append(sstables, []*SSTable{})
				}
				sstables[level] = append(sstables[level], sst)
			}
		}
		if !anyseen {
			break
		}
		level += 1
	}

	return sstables
}
func NewSSTableManager(dir string, bloomSize uint, indexInterval uint, l *zap.Logger) (*SSTableManager, error) {
	cm := NewCompactionManager(100)
	sm := &SSTableManager{
		dir:               dir,
		threshhold:        100,
		CompactionManager: cm,
		logger:            l,
	}
	sstables := sm.LoadCurrentSSTables()
	println("Loaded SSTables ")
	for i, level := range sstables {
		sm.logger.Info(fmt.Sprintf("level %d has %d sstables", i, len(level)))
	}
	cm.levels = sstables
	return sm, nil
}

func (sm *SSTableManager) Get(partKey []byte, clustering [][]byte, colName []byte) ([]byte, error) {
	for _, sst := range sm.CompactionManager.levels {
		for _, sstable := range sst {
			if sstable == nil {
				continue
			}
			cell, err := sstable.Get(partKey, colName, clustering...)
			if strings.Contains(err.Error(), "key not found") {
				continue
			}
			if err != nil {
				sm.logger.Error("Failed to get cell", zap.Error(err))
				return nil, err
			}
			if cell != nil {
				return cell.Value, nil
			}
		}
	}
	return nil, nil
}

func (sm *SSTableManager) CreateSSTable(entries []Cell) error {
	f, err := os.CreateTemp(sm.dir, "sst_0_")
	if err != nil {
		return err
	}
	p := path.Join(f.Name())
	err = f.Close()
	if err != nil {
		return err
	}
	sst, err := WriteSSTable(p, entries, 100, len(entries), 0.01)
	if err != nil {
		sm.logger.Error("Failed to create SSTable", zap.Error(err))
	}
	sm.CompactionManager.AddSSTable(sst)
	return nil
}

func (sm *SSTableManager) lenPerLevel(level int) int {

	return 0
}
