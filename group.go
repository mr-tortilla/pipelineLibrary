package pipeline

import (
	"context"
	"sync"
)

// NodeGroup запускает несколько нод и вызывает onDone
// когда все ноды завершились
type NodeGroup struct {
	nodes  []Node
	onDone func()
}

// NewNodeGroup создаёт группу нод с колбэком на завершение
func NewNodeGroup(onDone func(), nodes ...Node) *NodeGroup {
	return &NodeGroup{nodes: nodes, onDone: onDone}
}

func (g *NodeGroup) Add(nodes ...Node) {
	g.nodes = append(g.nodes, nodes...)
}

// Run запускает все ноды в группе
// После завершения всех нод вызывает onDone
func (g *NodeGroup) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, n := range g.nodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			n.Run(ctx)
		}(n)
	}
	wg.Wait()
	if g.onDone != nil {
		g.onDone()
	}
}
