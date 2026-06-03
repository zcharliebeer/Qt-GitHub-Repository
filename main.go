package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Widget represents a UI component.
type Widget struct {
	ID        int64
	Data      string
	Active    bool
	isDeleted bool
}

// WidgetPool manages reusable Widget instances to prevent frequent allocation/deallocation overhead.
type WidgetPool struct {
	mu      sync.Mutex
	widgets []*Widget
}

func NewWidgetPool() *WidgetPool {
	return &WidgetPool{
		widgets: make([]*Widget, 0),
	}
}

func (p *WidgetPool) Acquire(id int64, data string) *Widget {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.widgets {
		if !w.Active && !w.isDeleted {
			w.ID = id
			w.Data = data
			w.Active = true
			return w
		}
	}

	w := &Widget{
		ID:     id,
		Data:   data,
		Active: true,
	}
	p.widgets = append(p.widgets, w)
	return w
}

func (p *WidgetPool) Release(w *Widget) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w.Active = false
}

// UIThread simulates the main GUI thread.
type UIThread struct {
	eventQueue chan func()
	stopChan   chan struct{}
}

func NewUIThread() *UIThread {
	return &UIThread{
		eventQueue: make(chan func(), 1000),
		stopChan:   make(chan struct{}),
	}
}

func (ui *UIThread) Start() {
	go func() {
		for {
			select {
			case event := <-ui.eventQueue:
				event()
			case <-ui.stopChan:
				return
			}
		}
	}()
}

func (ui *UIThread) PostEvent(event func()) {
	ui.eventQueue <- event
}

func (ui *UIThread) Stop() {
	close(ui.stopChan)
}

// SafeDeallocator handles deferred deletion of widgets (simulating deleteLater).
type SafeDeallocator struct {
	pool       *WidgetPool
	deleteChan chan *Widget
}

func NewSafeDeallocator(pool *WidgetPool) *SafeDeallocator {
	sd := &SafeDeallocator{
		pool:       pool,
		deleteChan: make(chan *Widget, 1000),
	}
	go sd.processDeallocations()
	return sd
}

func (sd *SafeDeallocator) DeleteLater(w *Widget) {
	sd.deleteChan <- w
}

func (sd *SafeDeallocator) processDeallocations() {
	for w := range sd.deleteChan {
		// Decouple heavy teardown logic from the main thread
		sd.performHeavyTeardown(w)
		w.isDeleted = true
		sd.pool.Release(w)
	}
}

func (sd *SafeDeallocator) performHeavyTeardown(w *Widget) {
	// Simulate heavy teardown (e.g., disconnecting signals, saving state) asynchronously
	time.Sleep(1 * time.Millisecond)
}

func main() {
	fmt.Println("Starting UI Thread and Widget Lifecycle Simulation...")

	ui := NewUIThread()
	ui.Start()
	defer ui.Stop()

	pool := NewWidgetPool()
	deallocator := NewSafeDeallocator(pool)

	var activeWidgets int64
	var processedCycles int64

	// Debounce/Throttle simulation: trigger widget regeneration with a delay
	debounceTimer := time.NewTimer(150 * time.Millisecond)
	defer debounceTimer.Stop()

	// Stress test: Simulate rapid creation and destruction of 500+ widgets
	const widgetCount = 500
	var wg sync.WaitGroup
	wg.Add(widgetCount)

	startTime := time.Now()

	for i := 0; i < widgetCount; i++ {
		id := int64(i)
		// Simulate user interaction triggering widget creation
		ui.PostEvent(func() {
			// Offload non-UI work (e.g., fetching data) asynchronously
			go func(wID int64) {
				// Simulate data fetching
				time.Sleep(2 * time.Millisecond)
				data := fmt.Sprintf("Data for widget %d", wID)

				// Update UI on the main thread
				ui.PostEvent(func() {
					w := pool.Acquire(wID, data)
					atomic.AddInt64(&activeWidgets, 1)

					// Simulate immediate destruction/replacement (rapid switching)
					go func(widgetToDelete *Widget) {
						// Simulate some delay before user switches tab / triggers destruction
						time.Sleep(5 * time.Millisecond)
						ui.PostEvent(func() {
							deallocator.DeleteLater(widgetToDelete)
							atomic.AddInt64(&activeWidgets, -1)
							atomic.AddInt64(&processedCycles, 1)
							wg.Done()
						})
					}(w)
				})
			}(id)
		})
	}

	// Wait for all widgets to be processed and cleaned up
	wg.Wait()

	// Allow deallocator to finish processing remaining queue
	time.Sleep(100 * time.Millisecond)

	duration := time.Since(startTime)
	fmt.Printf("Successfully processed %d widget creation/destruction cycles in %v.\n", processedCycles, duration)
	fmt.Printf("Active widgets remaining: %d\n", atomic.LoadInt64(&activeWidgets))
	fmt.Println("UI Thread remained responsive throughout the process.")
}
