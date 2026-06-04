package event

import (
	"reflect"
	"sync"
)

type Event interface{}

type UnsubscribeFunc func()

type EventBus struct {
	mu     sync.RWMutex
	subs   map[reflect.Type]map[uint64]chan Event
	nextID uint64
}

func NewEventBus() *EventBus {
	return &EventBus{
		subs: make(map[reflect.Type]map[uint64]chan Event),
	}
}

func (b *EventBus) Publish(e Event) {
	t := reflect.TypeOf(e)

	b.mu.RLock()
	subs := b.subs[t]
	b.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (b *EventBus) Subscribe(e Event) (ch <-chan Event, unsubscribe UnsubscribeFunc) {
	c := make(chan Event, 32)
	t := reflect.TypeOf(e)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs[t] == nil {
		b.subs[t] = make(map[uint64]chan Event)
	}

	b.nextID++
	id := b.nextID

	b.subs[t][id] = c

	return c, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs[t], id)
	}
}
