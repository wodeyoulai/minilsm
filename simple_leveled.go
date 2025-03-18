package mini_lsm

import "errors"

type SimpleLeveledCompactionOptions struct {
	// lower files / upper files
	SizeRatioPercent uint32
	// L0->L1
	Level0FileNumCompactionTrigger int

	// l1->l2...
	ThresholdFilesCount uint32
	MaxLevels           uint32
}

func NewDefaultSimpleLeveledCompactionOptions() *SimpleLeveledCompactionOptions {
	return &SimpleLeveledCompactionOptions{
		SizeRatioPercent:               200, // default config
		Level0FileNumCompactionTrigger: 4,
		MaxLevels:                      7,
	}
}

type SimpleLeveledCompactionTask struct {
	upperLevel              uint32
	UpperLevelSSTIds        []uint32
	LowerLevel              uint32
	LowerLevelSSTIds        []uint32
	IsLowerLevelBottomLevel bool
	OutputSSTIds            []uint32
}

func (s *SimpleLeveledCompactionTask) UpperSstables() []uint32 {
	return s.UpperLevelSSTIds
}
func (s *SimpleLeveledCompactionTask) LowerSstables() []uint32 {
	return s.LowerLevelSSTIds
}
func (s *SimpleLeveledCompactionTask) UpperLevel() uint32 {
	return s.upperLevel
}
func (s *SimpleLeveledCompactionTask) OutputSstables() []uint32 {
	return s.OutputSSTIds
}
func (s *SimpleLeveledCompactionTask) SetOutputSstables(tables []uint32) {
	s.OutputSSTIds = tables
}
func (s *SimpleLeveledCompactionTask) TaskType() TaskType {
	return TaskTypeSimple
}

func (s *SimpleLeveledCompactionTask) CompactToBottomLevel() bool {
	return s.IsLowerLevelBottomLevel
}

type SimpleLeveledCompactionController struct {
	opts *SimpleLeveledCompactionOptions
}

func NewDefaultSimpleLeveledCompactionController(opt *SimpleLeveledCompactionOptions) *SimpleLeveledCompactionController {
	return &SimpleLeveledCompactionController{opt}
}

func (s *SimpleLeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) (CompactionTask, error) {

	// 1. First check whether the number of L0 files triggers compression
	if len(snapshot.l0SSTables) >= s.opts.Level0FileNumCompactionTrigger {
		// L0 -> L1 compression
		return &SimpleLeveledCompactionTask{
			upperLevel:              0,
			UpperLevelSSTIds:        snapshot.l0SSTables,
			LowerLevel:              1,
			LowerLevelSSTIds:        snapshot.levels[0].ssTables, // get all ssts in l1
			IsLowerLevelBottomLevel: len(snapshot.levels) == 1,
		}, nil
	}
	// 2. check the size ratio of each layer
	for level := 1; level < len(snapshot.levels); level++ {
		upperLevelFiles := len(snapshot.levels[level-1].ssTables)
		lowerLevelFiles := len(snapshot.levels[level].ssTables)

		if lowerLevelFiles == 0 {
			// 只有当上层文件数量达到阈值时才触发压缩
			if uint32(upperLevelFiles) >= s.opts.ThresholdFilesCount {
				return &SimpleLeveledCompactionTask{
					upperLevel:              uint32(level),
					UpperLevelSSTIds:        snapshot.levels[level-1].ssTables,
					LowerLevel:              uint32(level) + 1,
					LowerLevelSSTIds:        []uint32{}, //
					IsLowerLevelBottomLevel: level+1 == len(snapshot.levels),
				}, nil
			}
			// 否则不触发压缩
			continue
		}
		// If the upper layer is not empty, and the number of files in the lower layer/upper layer is < size_ratio_percent
		ratio := (lowerLevelFiles * 100) / upperLevelFiles
		if ratio < int(s.opts.SizeRatioPercent) {
			return &SimpleLeveledCompactionTask{
				upperLevel:              uint32(level),
				UpperLevelSSTIds:        snapshot.levels[level-1].ssTables,
				LowerLevel:              uint32(level) + 1,
				LowerLevelSSTIds:        snapshot.levels[level].ssTables,
				IsLowerLevelBottomLevel: level+1 == len(snapshot.levels),
			}, nil
		}
	}
	// no tasks that require compression
	return nil, nil

}

func (s *SimpleLeveledCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task CompactionTask, inRecovery bool) error {
	// List of files to be removed
	realTask := &SimpleLeveledCompactionTask{}
	if v, ok := task.(*SimpleLeveledCompactionTask); ok {
		realTask = v
	} else {
		return errors.New("invalid compaction task")
	}
	filesToRemove := make([]uint32, 0)

	// Add all files involved in compaction to the removal list
	filesToRemove = append(filesToRemove, realTask.UpperLevelSSTIds...)
	filesToRemove = append(filesToRemove, realTask.LowerLevelSSTIds...)

	// Handle L0 to L1 compaction
	if realTask.upperLevel == 0 {
		// Remove corresponding SST files from L0
		newL0Files := make([]uint32, 0)
		for _, file := range snapshot.l0SSTables {
			if !contains(realTask.UpperLevelSSTIds, file) {
				newL0Files = append(newL0Files, file)
			}
		}

		// Remove corresponding SST files from L1
		newL1Files := make([]uint32, 0)
		for _, file := range snapshot.levels[0].ssTables {
			if !contains(realTask.LowerLevelSSTIds, file) {
				newL1Files = append(newL1Files, file)
			}
		}
		// Add newly generated SST files to L1
		newL1Files = append(newL1Files, realTask.OutputSSTIds...)

		snapshot.mu.Lock()
		snapshot.l0SSTables = newL0Files
		snapshot.levels[0].ssTables = newL1Files
		snapshot.mu.Unlock()
	} else {
		// Handle compaction between other levels
		upperLevel := realTask.upperLevel
		lowerLevel := realTask.LowerLevel

		// Remove files from upper level
		newUpperFiles := make([]uint32, 0)
		for _, file := range snapshot.levels[upperLevel-1].ssTables {
			if !contains(realTask.UpperLevelSSTIds, file) {
				newUpperFiles = append(newUpperFiles, file)
			}
		}

		// Remove files from lower level
		newLowerFiles := make([]uint32, 0)
		for _, file := range snapshot.levels[lowerLevel-1].ssTables {
			if !contains(realTask.LowerLevelSSTIds, file) {
				newLowerFiles = append(newLowerFiles, file)
			}
		}
		// Add newly generated SST files to lower level
		newLowerFiles = append(newLowerFiles, realTask.OutputSSTIds...)
		snapshot.mu.Lock()
		snapshot.levels[upperLevel-1].ssTables = newUpperFiles
		snapshot.levels[lowerLevel-1].ssTables = newLowerFiles
		snapshot.mu.Unlock()
	}
	return nil
}

func (s *SimpleLeveledCompactionController) FlushToL0() bool {
	//TODO implement me
	return true
}
func toUint(vals []uint16) []uint {
	result := make([]uint, len(vals))
	for i, v := range vals {
		result[i] = uint(v)
	}
	return result
}

// Helper function: Check if an array contains a specific element
func contains(arr []uint32, target uint32) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}
	return false
}
