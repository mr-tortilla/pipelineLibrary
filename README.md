# pipelineLibrary

Легковесная библиотека для построения пайплайнов потоковой обработки данных на Go.

## Требования

- Go 1.25+

## Установка

```bash
go get github.com/mr-tortilla/pipelineLibrary
```

## Концепция

Пайплайн - это направленный граф из нод. Каждая нода читает данные из входных каналов,
обрабатывает их и пишет результат в выходные каналы. Ноды выполняются параллельно.
Библиотека берёт на себя создание горутин, синхронизацию и закрытие каналов.

Библиотека предоставляет два строительных блока:

- **Node** - интерфейс который должна реализовать каждая нода
- **Pipeline** - соединяет ноды, запускает их и управляет жизненным циклом каналов

## Использование

### 1. Реализуйте интерфейс Node

```go
type MyNode struct {
    inputs  []chan any
    outputs []chan any
}

func (n *MyNode) SetInputs(inputs []chan any)   { n.inputs = inputs }
func (n *MyNode) SetOutputs(outputs []chan any) { n.outputs = outputs }

func (n *MyNode) Run(ctx context.Context) {
    in  := n.inputs[0]
    out := n.outputs[0]

    for {
        select {
        case <-ctx.Done():
            return
        case val, ok := <-in:
            if !ok {
                return
            }
            out <- process(val.(string))
        }
    }
}
```

### 2. Соедините ноды и запустите пайплайн

```go
ch1 := make(chan any)
ch2 := make(chan any)

nodeA := &MyNode{}
nodeB := &MyNode{}
nodeC := &MyNode{}

p := pipeline.NewPipeline()
p.Connect(nodeA, nodeB, ch1)
p.Connect(nodeB, nodeC, ch2)
p.Add(nodeA, nodeB, nodeC)

ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()

p.Exec(ctx)
p.Wait()
```

### Параллельное выполнение

Для параллельной обработки передайте один канал нескольким нодам:

```go
ch := make(chan any)

p.Connect(source, worker1, ch)
p.Connect(source, worker2, ch)
p.Connect(source, worker3, ch)
```

Go автоматически распределяет значения из канала между свободными воркерами.

### Остановка

```go
// через контекст
cancel()

// или напрямую
p.Stop()
```

## Пример

Демонстрационное приложение - вычисление MD5-хешей файлов в директории:
[pipelineTest](https://github.com/mr-tortilla/pipelineTest)