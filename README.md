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

Библиотека предоставляет три строительных блока:

- **Node** - интерфейс который должна реализовать каждая нода
- **Pipeline** - запускает ноды и управляет их жизненным циклом
- **NodeGroup** - запускает группу нод параллельно и вызывает колбэк когда все завершились

## Использование

### 1. Реализуйте интерфейс Node

```go
type MyNode struct {
    In  <-chan string
    Out chan<- string
}

func (n *MyNode) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case val, ok := <-n.In:
            if !ok {
                return
            }
            n.Out <- process(val)
        }
    }
}
```

### 2. Соедините ноды каналами

```go
ch1 := make(chan string)
ch2 := make(chan string)

nodeA := &MyNode{Out: ch1}
nodeB := &MyNode{In: ch1, Out: ch2}
nodeC := &MyNode{In: ch2}
```

### 3. Запустите пайплайн

```go
p := pipeline.New()
p.Add(nodeA, nodeB, nodeC)

ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()

p.Run(ctx)
p.Wait()
```

### Параллельное выполнение через NodeGroup

```go
out := make(chan Result)

group := pipeline.NewNodeGroup(
    func() { close(out) }, // вызывается когда все ноды завершились
    &WorkerNode{Out: out},
    &WorkerNode{Out: out},
    &WorkerNode{Out: out},
)

p.Add(group)
```

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