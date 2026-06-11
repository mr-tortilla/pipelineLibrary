package pipeline

import "context"

// Node представляет единицу обработки данных в пайплайне.
// Библиотека передаёт каналы через SetInputs и SetOutputs перед запуском.
type Node interface {
	Run(ctx context.Context)
	SetInputs(inputs []chan any)
	SetOutputs(outputs []chan any)
}
