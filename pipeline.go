package pipeline

import (
	"context"
	"sync"
)

// Pipeline управляет набором нод
type Pipeline struct {
	nodes  []Node
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New создает новый Pipeline
func New() *Pipeline {
	return &Pipeline{}
}

// Add добавляет одну или несколько нод в пайплайн
func (p *Pipeline) Add(nodes ...Node) {
	p.nodes = append(p.nodes, nodes...)
}

// Run запускает все ноды конкурентно
// Отмена ctx сообщит нодам об остановке
func (p *Pipeline) Run(ctx context.Context) {
	for _, node := range p.nodes {
		p.wg.Add(1)
		go func(n Node) {
			defer p.wg.Done()
			n.Run(ctx)
		}(node)
	}
}

// Stop останавливает все ноды, отменяя контекст
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// Wait блокирует выполнение до завершения всех нод
func (p *Pipeline) Wait() {
	p.wg.Wait()
}
