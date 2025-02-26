package mini_lsm

type TaskType int

const (
	TaskTypeLeveled TaskType = iota
	TaskTypeTiered
	TaskTypeSimple
	TaskTypeForceFullCompaction
)

func (c TaskType) String() string {
	switch c {
	case TaskTypeLeveled:
		return "Leveled"
	case TaskTypeTiered:
		return "Tiered"
	case TaskTypeSimple:
		return "Simple"
	case TaskTypeForceFullCompaction:
		return "NoCompaction"
	default:
		return "Unknown"
	}
}

type LeveledCompactionOptions struct {
}
type TieredCompactionOptions struct {
}

type ForceFullCompactionOptions struct {
}

// CompactionOptions 压缩选项配置
type CompactionOptions struct {
	// 策略类型
	Strategy TaskType

	// 各种策略的具体选项
	// 只有一个会被使用，其他都是 nil
	LeveledOpts   *LeveledCompactionOptions
	TieredOpts    *TieredCompactionOptions
	SimpleOpts    *SimpleLeveledCompactionOptions
	ForceFullOpts *ForceFullCompactionOptions
}

// CompactionTask represents different types of compaction tasks
type CompactionTask interface {
	TaskType() TaskType
	CompactToBottomLevel() bool
	UpperSstables() []uint32
	LowerSstables() []uint32
	OutputSstables() []uint32
	UpperLevel() uint32
}

// CompactionController interface defines the common behavior for all compaction strategies
type CompactionController interface {
	GenerateCompactionTask(snapshot *LsmStorageState) (CompactionTask, error)
	ApplyCompactionResult(snapshot *LsmStorageState, task CompactionTask, inRecovery bool) error
	FlushToL0() bool
}

type ForceFullCompactionController struct {
	opts ForceFullCompactionOptions
}

func (f *ForceFullCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) (*CompactionTask, error) {
	//TODO implement me
	panic("implement me")
}

func (f *ForceFullCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *CompactionTask, inRecovery bool) ([]uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (f *ForceFullCompactionController) FlushToL0() bool {
	//TODO implement me
	panic("implement me")
}

// Concrete implementations of CompactionController

type LeveledCompactionController struct {
	opts LeveledCompactionOptions
}

func (l *LeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) (*CompactionTask, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LeveledCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *CompactionTask, output []uint64, inRecovery bool) (*LsmStorageState, []uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LeveledCompactionController) FlushToL0() bool {
	//TODO implement me
	panic("implement me")
}

type TieredCompactionController struct {
	opts TieredCompactionOptions
}
