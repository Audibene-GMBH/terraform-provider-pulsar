package pulsar

import (
	"sync"

	"github.com/streamnative/pulsarctl/pkg/pulsar"
)

// This is a hack because pulsarctl does not support an instance talking to multiple versions
// and pulsar's admin API does not answer /v2 resources on /v3 endpoints.
// Ideally the meta should contain both clients
var (
	clientV3Lock = sync.Mutex{}
	clientToV3   = make(map[pulsar.Client]pulsar.Client)
)

func pushClientV3(originClient pulsar.Client, clientV3 pulsar.Client) {
	clientV3Lock.Lock()
	defer clientV3Lock.Unlock()
	clientToV3[originClient] = clientV3
}

func getClientV3(clientV2 pulsar.Client) pulsar.Client {
	// clientV3Lock.Lock()
	// defer clientV3Lock.Unlock()
	if clientV3, ok := clientToV3[clientV2]; ok {
		return clientV3
	}
	panic("Should not happen")
}

func getClientV3FromMeta(meta interface{}) pulsar.Client {
	originClient, ok := meta.(pulsar.Client)
	if !ok {
		panic("Should not happen")
	}
	return getClientV3(originClient)
}
