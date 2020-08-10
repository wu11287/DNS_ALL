package model

type ConsensusI interface {
	Start(done chan uint)
	Run(done chan uint)
	IsLeader() bool
}
