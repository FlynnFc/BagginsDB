package database

import (
	"fmt"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"
)

type sstableManager struct {
	dir               string
	CompactionManager *compactionManager
	threshhold        int
	sparseInterval    int
	fpRate            float32
	logger            *zap.Logger
}

func (sm *sstableManager) LoadCurrentsstables() [][]*sstable {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		sm.logger.Error("Failed to read sstable dir", zap.Error(err))
	}

	sstables := [][]*sstable{}
	sstables = append(sstables, []*sstable{})
	level := 0
	for {
		anyseen := false
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), fmt.Sprintf("sst_%d", level)) {
				anyseen = true
				sst, err := loadSSTable(path.Join(sm.dir, e.Name()))
				if err != nil {
					sm.logger.Error("Failed to load sstable", zap.Error(err))
				}
				if len(sstables) < level+1 {
					sstables = append(sstables, []*sstable{})
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

func newSSTableManager(dir string, bloomSize uint, indexInterval uint, l *zap.Logger) (*sstableManager, error) {
	cm := newCompactionManager(100)
	sm := &sstableManager{
		dir:               dir,
		threshhold:        100,
		CompactionManager: cm,
		logger:            l,
	}
	sstables := sm.LoadCurrentsstables()
	for i, level := range sstables {
		sm.logger.Info(fmt.Sprintf("level %d has %d sstables", i, len(level)))
	}
	cm.levels = sstables
	return sm, nil
}

func (sm *sstableManager) Get(partKey []byte, clustering [][]byte, colName []byte) ([]byte, error) {
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

func (sm *sstableManager) CreateSSTable(entries []Cell) error {
	f, err := os.CreateTemp(sm.dir, "sst_0_")
	if err != nil {
		return err
	}
	p := path.Join(f.Name())
	err = f.Close()
	if err != nil {
		return err
	}
	sst, err := writeSSTable(p, entries, 100, len(entries), 0.01)
	if err != nil {
		sm.logger.Error("Failed to create sstable", zap.Error(err))
	}
	sm.CompactionManager.AddSSTable(sst)
	return nil
}
