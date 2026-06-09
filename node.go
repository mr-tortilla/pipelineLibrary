package pipeline

import "context"

// Node - логическая единица в пайплайне
// Может принимать каналы для входных данных и выходных
type Node interface {
	Run(ctx context.Context)
}
