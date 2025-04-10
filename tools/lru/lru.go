package lru

import (
	"errors"
	"sync"
	"sync/atomic"
)

type linkNode struct {
	pre, next *linkNode
	key       string
	value     any
}

type LRUCache struct {
	capacity   int
	count      atomic.Int32
	mu         sync.RWMutex
	dic        sync.Map
	head, tail *linkNode
	aheadC     chan *linkNode
	done       chan struct{}
}

func Constructor(capacity int) (*LRUCache, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be positive")
	}
	t := &LRUCache{
		capacity: capacity,
		count:    atomic.Int32{},
		mu:       sync.RWMutex{},
		dic:      sync.Map{},
		head:     nil,
		tail:     nil,
		aheadC:   make(chan *linkNode, capacity),
		done:     make(chan struct{}),
	}
	go t.moveToHead()
	return t, nil
}

func (l *LRUCache) Get(key string) (any, error) {
	if v, ok := l.dic.Load(key); ok {
		node := v.(*linkNode)
		l.mu.Lock()
		l.toHead(node)
		l.mu.Unlock()
		return node.value, nil
	}
	return nil, errors.New("key not found")
}

func (l *LRUCache) Put(key string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 检查key是否已存在
	if v, ok := l.dic.Load(key); ok {
		node := v.(*linkNode)
		node.value = value
		select {
		case l.aheadC <- node:
		default:
			// 如果channel已满，跳过移动操作
		}
		return
	}

	if int(l.count.Load()) == l.capacity {
		l.subTailAndMoveToHead(key, value)
		return
	}
	newNode := &linkNode{key: key, value: value}
	l.addToHead(newNode)
	l.count.Add(1)
	return
}

func (l *LRUCache) subTailAndMoveToHead(key string, value any) {
	if l.tail == nil {
		return
	}
	t := l.tail
	oldKey := t.key
	t.key = key
	t.value = value
	l.aheadC <- t
	l.dic.Delete(oldKey)
	l.dic.Store(key, t)
}

func (l *LRUCache) addToHead(n *linkNode) {
	l.dic.Store(n.key, n)
	if l.head == nil {
		l.head = n
		l.tail = n
		return
	}
	l.head.pre = n
	n.next = l.head
	l.head = n

}
func (l *LRUCache) Close() {
	close(l.done)
}
func (l *LRUCache) Clear() {
	close(l.done)
}
func (l *LRUCache) Size() int32 {
	return l.count.Load()
}
func (l *LRUCache) moveToHead() {
	for {
		select {
		case m := <-l.aheadC:
			if m == nil {
				break
			}
			l.toHead(m)
		case <-l.done:
			return
		}
	}
}

func (l *LRUCache) toHead(n *linkNode) {
	if n == l.head {
		return
	}
	if n == l.tail {
		l.tail = n.pre
	}
	if n.pre != nil {
		n.pre.next = n.next
	}
	if n.next != nil {
		n.next.pre = n.pre
	}
	// move the node to the head
	n.next = l.head
	n.pre = nil
	l.head.pre = n
	l.head = n

}
