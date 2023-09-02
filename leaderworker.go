package leaderworker

type Node struct {
	Id        string
	LastAlive int64
}

type LeaderWorker interface {
	Keep() <-chan error
	Race() (<-chan bool, <-chan error)
	List() ([]Node, error)
	Remove(id ...string) (int64, error)
	Stop()
}
