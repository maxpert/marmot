package lib

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/db"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/rs/zerolog/log"
)

var MaxLogEntries = uint64(10_000)

type RaftServer struct {
	bindAddress string
	nodeID      uint64
	metaPath    string
	lock        *sync.RWMutex
	nodeHost    *dragonboat.NodeHost
	database    *db.SqliteStreamDB

	clusterStateMachine map[uint64]*SQLiteStateMachine
	nodeUser            map[uint64]dragonboat.INodeUser
	nodeMap             map[uint64]map[uint64]uint64
	clusterMap          map[uint64]uint64
}

func NewRaftServer(
	bindAddress string,
	nodeID uint64,
	metaPath string,
	database *db.SqliteStreamDB,
) *RaftServer {
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.WARNING)
	logger.GetLogger("config").SetLevel(logger.WARNING)

	return &RaftServer{
		bindAddress: bindAddress,
		nodeID:      nodeID,
		database:    database,
		metaPath:    metaPath,
		lock:        &sync.RWMutex{},

		nodeUser:            map[uint64]dragonboat.INodeUser{},
		clusterMap:          make(map[uint64]uint64),
		nodeMap:             map[uint64]map[uint64]uint64{},
		clusterStateMachine: map[uint64]*SQLiteStateMachine{},
	}
}

func (r *RaftServer) Init() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	metaAbsPath := fmt.Sprintf("%s/node-%d", r.metaPath, r.nodeID)
	hostConfig := config.NodeHostConfig{
		WALDir:            metaAbsPath,
		NodeHostDir:       metaAbsPath,
		RTTMillisecond:    300,
		RaftAddress:       r.bindAddress,
		RaftEventListener: r,
	}

	factory := NewSQLiteLogDBFactory(r.metaPath, r.nodeID)
	hostConfig.Expert = config.ExpertConfig{
		LogDBFactory: factory,
	}

	nodeHost, err := dragonboat.NewNodeHost(hostConfig)
	if err != nil {
		return err
	}

	r.nodeHost = nodeHost
	return nil
}

func (r *RaftServer) BindCluster(initMembers string, join bool, clusterIDs ...uint64) error {
	initialMembers := parsePeersMap(initMembers)
	if !join {
		initialMembers[r.nodeID] = r.bindAddress
	}

	for _, clusterID := range clusterIDs {
		cfg := r.config(clusterID)
		log.Debug().
			Uint64("cluster", clusterID).
			Uint64("node", r.nodeID).
			Msg("Starting cluster...")
		err := r.nodeHost.StartOnDiskCluster(initialMembers, join, r.stateMachineFactory, cfg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RaftServer) RequestSnapshot(timeout time.Duration) (uint64, uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for clusterID, nodeID := range r.GetClusterMap() {
		if nodeID == r.nodeID || nodeID == 0 {
			continue
		}

		ret, err := r.nodeHost.SyncRequestSnapshot(ctx, clusterID, dragonboat.SnapshotOption{})

		if err != nil {
			return 0, 0, err
		}

		return ret, clusterID, nil
	}

	return 0, 0, nil
}

func (r *RaftServer) AddNode(peerID uint64, address string, clusterIDs ...uint64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, clusterID := range clusterIDs {
		mem, err := r.nodeHost.SyncGetClusterMembership(ctx, clusterID)
		if err != nil {
			return err
		}

		err = r.nodeHost.SyncRequestAddNode(ctx, clusterID, peerID, address, mem.ConfigChangeID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RaftServer) ShuffleCluster(nodes ...uint64) error {
	nodeMap := r.GetNodeMap()
	allNodeList := make([]uint64, 0)
	if len(nodes) > 0 {
		allNodeList = append(allNodeList, nodes...)
	}

	for nodeID := range nodeMap {
		allNodeList = append(allNodeList, nodeID)
	}

	rand.Seed(time.Now().UnixNano())

	for nodeIndex, nodeID := range allNodeList {
		clusterIDs := nodeMap[nodeID]
		for _, clusterID := range clusterIDs {
			newOwnerNodeIndex := rand.Uint64() % uint64(len(allNodeList))
			if newOwnerNodeIndex != uint64(nodeIndex) {
				newOwnerNodeID := allNodeList[newOwnerNodeIndex]

				log.Debug().Msg(fmt.Sprintf("Moving %v from %v to %v", clusterID, nodeID, newOwnerNodeID))
				if err := r.TransferClusters(newOwnerNodeID, clusterID); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *RaftServer) GetSnapshotStateMachine() *SQLiteStateMachine {
	r.lock.RLock()
	defer r.lock.RUnlock()

	for _, sm := range r.clusterStateMachine {
		if sm.IsSnapshotEnabled() {
			return sm
		}
	}

	return nil
}

func (r *RaftServer) GetNodeMap() map[uint64][]uint64 {
	nodeMap := map[uint64][]uint64{}

	for nodeID, clusterMap := range r.nodeMap {
		clusters := make([]uint64, 0)
		for clusterID := range clusterMap {
			clusters = append(clusters, clusterID)
		}

		nodeMap[nodeID] = clusters
	}

	return nodeMap
}

func (r *RaftServer) TransferClusters(toPeerID uint64, clusterIDs ...uint64) error {
	for _, cluster := range clusterIDs {
		err := r.nodeHost.RequestLeaderTransfer(cluster, toPeerID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RaftServer) GetActiveClusters() []uint64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	ret := make([]uint64, 0)
	for clusterID, nodeID := range r.clusterMap {
		if nodeID != 0 {
			ret = append(ret, clusterID)
		}
	}

	return ret
}

func (r *RaftServer) GetClusterMap() map[uint64]uint64 {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.clusterMap
}

func (r *RaftServer) Propose(key uint64, data []byte, dur time.Duration) (*dragonboat.RequestResult, error) {
	clusterIds := r.GetActiveClusters()
	clusterIndex := uint64(1)
	if len(clusterIds) == 0 {
		return nil, errors.New("cluster not ready")
	}

	clusterIndex = key % uint64(len(clusterIds))
	clusterId := clusterIds[clusterIndex]
	nodeUser, err := r.getNodeUser(clusterId)
	if err != nil {
		return nil, err
	}

	session := r.nodeHost.GetNoOPSession(clusterId)
	req, err := nodeUser.Propose(session, data, dur)
	if err != nil {
		return nil, err
	}

	res := <-req.ResultC()
	return &res, err
}

func (r *RaftServer) LeaderUpdated(info raftio.LeaderInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if info.LeaderID == 0 {
		previousLeader := r.clusterMap[info.ClusterID]
		delete(r.clusterMap, info.ClusterID)
		r.mutateNodeMap(previousLeader, func(m map[uint64]uint64) {
			delete(m, info.ClusterID)
		})
	} else {
		r.clusterMap[info.ClusterID] = info.LeaderID
		r.mutateNodeMap(info.LeaderID, func(m map[uint64]uint64) {
			m[info.ClusterID] = info.Term
		})
	}
}

func (r *RaftServer) mutateNodeMap(nodeID uint64, f func(map[uint64]uint64)) {
	m, ok := r.nodeMap[nodeID]
	if !ok {
		m = make(map[uint64]uint64, 0)
	}

	f(m)
	r.nodeMap[nodeID] = m

	if len(m) <= 0 {
		delete(r.nodeMap, nodeID)
	}
}

func (r *RaftServer) stateMachineFactory(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
	r.lock.Lock()
	defer r.lock.Unlock()

	sm, ok := r.clusterStateMachine[clusterID]
	if !ok {
		sm = NewDBStateMachine(clusterID, nodeID, r.database, r.metaPath, clusterID == 1)
		r.clusterStateMachine[clusterID] = sm
	}

	return sm
}

func (r *RaftServer) getNodeUser(clusterID uint64) (dragonboat.INodeUser, error) {
	r.lock.RLock()
	if val, ok := r.nodeUser[clusterID]; ok {
		r.lock.RUnlock()
		return val, nil
	}

	r.lock.RUnlock()
	r.lock.Lock()
	defer r.lock.Unlock()

	val, err := r.nodeHost.GetNodeUser(clusterID)
	if err != nil {
		return nil, err
	}
	r.nodeUser[clusterID] = val
	return val, nil
}

func (r *RaftServer) config(clusterID uint64) config.Config {
	return config.Config{
		NodeID:                  r.nodeID,
		ClusterID:               clusterID,
		ElectionRTT:             10,
		HeartbeatRTT:            1,
		CheckQuorum:             true,
		SnapshotEntries:         MaxLogEntries,
		CompactionOverhead:      0,
		EntryCompressionType:    config.Snappy,
		SnapshotCompressionType: config.Snappy,
	}
}

func parsePeersMap(peersAddrs string) map[uint64]string {
	peersMap := make(map[uint64]string)
	if peersAddrs == "" {
		return peersMap
	}

	for _, peer := range strings.Split(peersAddrs, ",") {
		peerInf := strings.Split(peer, "@")
		peerShard, err := strconv.ParseUint(peerInf[0], 10, 64)
		if err != nil {
			continue
		}

		peersMap[peerShard] = peerInf[1]
	}

	log.Debug().Msg(fmt.Sprintf("Peer map %v", peersMap))
	return peersMap
}
