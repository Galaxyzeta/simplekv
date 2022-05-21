package util

type IntGetter interface {
	Get() int
}

type IntHashQueue[V IntGetter] struct {
	hash  map[int]*V
	queue []*V
}

func NewIntHashQueue[T IntGetter]() *IntHashQueue[T] {
	return NewIntHashQueueWithCapacity[T](0, 0)
}

func NewIntHashQueueWithCapacity[T IntGetter](qsize, mapsize int) *IntHashQueue[T] {
	return &IntHashQueue[T]{
		hash:  make(map[int]*T, mapsize),
		queue: make([]*T, 0, qsize),
	}
}

func (hq *IntHashQueue[V]) Enqueue(v *V) {
	hq.hash[(*v).Get()] = v
	hq.queue = append(hq.queue, v)
}

func (hq *IntHashQueue[V]) Dequeue() *V {
	if hq.Size() == 0 {
		return nil
	}
	ret := hq.queue[0]
	hq.queue = hq.queue[1:]
	delete(hq.hash, (*ret).Get())
	return ret
}

func (hq *IntHashQueue[V]) Head() *V {
	if hq.Size() == 0 {
		return nil
	}
	first := hq.queue[0]
	return first
}

func (hq *IntHashQueue[V]) TryComplete(canComplete func(*V) bool) {
	for {
		h := hq.Head()
		if h == nil {
			break
		}
		if canComplete(h) {
			hq.Dequeue()
		} else {
			break
		}
	}
}

// Iterate through the hashqueue, if fn(v) is false, stop iteration.
func (hq *IntHashQueue[V]) Traverse(fn func(*V) bool) {
	for i := 0; i < hq.Size(); i++ {
		if !fn(hq.queue[i]) {
			return
		}
	}
}

func (hq *IntHashQueue[V]) Get(k int) *V {
	return hq.hash[k]
}

func (hq *IntHashQueue[V]) Size() int {
	return len(hq.queue)
}
