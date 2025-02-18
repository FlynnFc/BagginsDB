package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
)

// compactionManager holds SSTables organized into levels.
type compactionManager struct {
	levels    [][]*sstable
	threshold int // maximum number of SSTables in a level before compaction triggers.
}

// newCompactionManager creates a new compaction manager.
func newCompactionManager(threshold int) *compactionManager {
	return &compactionManager{
		levels:    make([][]*sstable, 1), // start with level 0
		threshold: threshold,
	}
}

// AddSSTable adds an SSTable to level 0 and triggers compaction if needed.
func (cm *compactionManager) AddSSTable(sst *sstable) error {
	if sst == nil {
		return errors.New("nil SSTable provided")
	}
	// Append the SSTable to level 0.
	cm.levels[0] = append(cm.levels[0], sst)

	// Iterate over each level.
	for i, level := range cm.levels {
		if len(level) >= cm.threshold {
			// Start compaction.
			merged, err := mergeSSTables(level, path.Dir(sst.filePath), i)
			if err != nil {
				return err
			}

			// Clear the current level.
			cm.levels[i] = nil

			// Ensure the next level exists.
			if len(cm.levels) < i+2 {
				cm.levels = append(cm.levels, []*sstable{})
			}

			// Append the merged SSTable to the next level.
			cm.levels[i+1] = append(cm.levels[i+1], merged)
		}
	}
	return nil
}

// mergeSSTables merges multiple SSTables into one.
func mergeSSTables(ssts []*sstable, dir string, currLevel int) (*sstable, error) {
	println("merging SSTables")
	var allCells []Cell
	for _, sst := range ssts {
		cells, err := sst.ReadAllCells()
		if err != nil {
			return nil, err
		}
		allCells = append(allCells, cells...)
	}
	// Sort cells by composite key.
	sort.Slice(allCells, func(i, j int) bool {
		return bytes.Compare(allCells[i].CompositeKey(), allCells[j].CompositeKey()) < 0
	})
	// Deduplicate cells (if keys repeat, keep the last occurrence our timestamp has come in handy ☺️).
	var dedup []Cell
	seen := make(map[string]bool)
	for i := len(allCells) - 1; i >= 0; i-- {
		keyStr := string(allCells[i].CompositeKey())
		if !seen[keyStr] {
			seen[keyStr] = true
			dedup = append([]Cell{allCells[i]}, dedup...)
		}
	}
	newLevel := currLevel + 1
	f, err := os.CreateTemp(dir, fmt.Sprintf("sst_%d_", newLevel))
	if err != nil {
		return nil, err
	}
	p := path.Join(dir, f.Name())
	err = f.Close()
	if err != nil {
		return nil, err
	}

	return writeSSTable(p, dedup, 10, len(dedup), 0.01)
}
