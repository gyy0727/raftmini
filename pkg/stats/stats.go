package stats

import "go.uber.org/zap"



var (
	logger,_ = zap.NewDevelopment()
)

type Stats interface {
	
	SelfStats() []byte
	
	LeaderStats() []byte
	
	StoreStats() []byte
}
