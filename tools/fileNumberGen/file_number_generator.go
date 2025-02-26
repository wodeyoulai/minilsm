package fileNumberGen

import "sync"

type FileNumberGenerator struct {
	mu       sync.Mutex
	nextId   uint64
	levelIds map[int]uint64 // 每层的最大ID
}

func (g *FileNumberGenerator) NextGlobalId() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextId++
	return g.nextId
}

func (g *FileNumberGenerator) NextLevelId(level int) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.levelIds[level]++
	return g.levelIds[level]
}
