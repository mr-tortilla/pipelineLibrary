package pipeline

import (
	"context"
	"sync"
)

// Pipeline управляет набором нод и координирует их выполнение
type Pipeline struct {
	nodes   []Node
	inputs  map[Node][]chan any
	outputs map[Node][]chan any
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

// NewPipeline создаёт новый Pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{
		inputs:  make(map[Node][]chan any),
		outputs: make(map[Node][]chan any),
	}
}

// Add добавляет одну или несколько нод в пайплайн
func (p *Pipeline) Add(nodes ...Node) {
	p.nodes = append(p.nodes, nodes...)
}

// Connect соединяет две ноды через переданный канал
// ch добавляется в выходы from и входы to
func (p *Pipeline) Connect(from, to Node, ch chan any) {
	// добавляем канал в outputs from только если его там ещё нет
	found := false
	for _, existing := range p.outputs[from] {
		if existing == ch {
			found = true
			break
		}
	}
	if !found {
		p.outputs[from] = append(p.outputs[from], ch)
	}

	// входы можно дублировать — каждая нода читает из своего среза
	p.inputs[to] = append(p.inputs[to], ch)
}

// Exec запускает все ноды конкурентно
// Библиотека сама закрывает каналы когда все писатели завершились
func (p *Pipeline) Exec(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)

	// считаем сколько нод пишет в каждый канал
	writers := make(map[chan any]*sync.WaitGroup)
	for _, outputs := range p.outputs {
		for _, ch := range outputs {
			if _, ok := writers[ch]; !ok {
				writers[ch] = &sync.WaitGroup{}
			}
			writers[ch].Add(1)
		}
	}

	// закрываем канал когда все писатели завершились
	for ch, wg := range writers {
		ch, wg := ch, wg
		go func() {
			wg.Wait()
			close(ch)
		}()
	}

	// передаём каналы нодам и запускаем
	for _, node := range p.nodes {
		node.SetInputs(p.inputs[node])
		node.SetOutputs(p.outputs[node])

		p.wg.Add(1)
		go func(n Node) {
			defer p.wg.Done()
			defer func() {
				// декрементируем счётчик для каждого выходного канала
				for _, ch := range p.outputs[n] {
					writers[ch].Done()
				}
			}()
			n.Run(ctx)
		}(node)
	}
}

// Stop останавливает все ноды отменяя контекст
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// Wait блокирует выполнение до завершения всех нод
func (p *Pipeline) Wait() {
	p.wg.Wait()
}
