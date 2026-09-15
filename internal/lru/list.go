package lru

// entryList is a doubly linked list threaded through entry itself, so storing an
// entry costs one allocation instead of two (container/list adds an Element and an
// interface conversion). The zero value is an empty list. Not safe for concurrent use.
type entryList struct {
	root entry // sentinel: root.next is the front, root.prev the back
	len  int
}

func (l *entryList) lazyInit() {
	if l.root.next == nil {
		l.root.next, l.root.prev = &l.root, &l.root
	}
}

// Init empties the list.
func (l *entryList) Init() {
	l.root.next, l.root.prev = &l.root, &l.root
	l.len = 0
}

// Len returns the number of entries.
func (l *entryList) Len() int { return l.len }

// Front returns the first entry or nil.
func (l *entryList) Front() *entry {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

// Back returns the last entry or nil.
func (l *entryList) Back() *entry {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// Next returns the entry after e or nil.
func (e *entry) Next() *entry {
	if n := e.next; e.list != nil && n != &e.list.root {
		return n
	}
	return nil
}

// Prev returns the entry before e or nil.
func (e *entry) Prev() *entry {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

func (l *entryList) insertAfter(e, at *entry) {
	e.prev, e.next = at, at.next
	at.next.prev = e
	at.next = e
	e.list = l
	l.len++
}

func (l *entryList) unlink(e *entry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	l.len--
}

// PushFront inserts e at the front and returns it.
func (l *entryList) PushFront(e *entry) *entry {
	l.lazyInit()
	l.insertAfter(e, &l.root)
	return e
}

// MoveToFront moves e, which must be in l, to the front.
func (l *entryList) MoveToFront(e *entry) {
	if e.list != l || l.root.next == e {
		return
	}
	l.unlink(e)
	l.insertAfter(e, &l.root)
}

// Remove unlinks e, which must be in l, and returns it.
func (l *entryList) Remove(e *entry) *entry {
	if e.list == l {
		l.unlink(e)
		e.prev, e.next, e.list = nil, nil, nil
	}
	return e
}
