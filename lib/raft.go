package lib

import (
    "fmt"
    "strconv"
    "strings"

    "github.com/lni/dragonboat/v3"
    "github.com/lni/dragonboat/v3/config"
    "github.com/lni/dragonboat/v3/logger"
    "github.com/lni/dragonboat/v3/statemachine"
    "marmot/db"
)

func InitRaft(
    bindAddress string,
    nodeID uint64,
    clusterID uint64,
    metaPath string,
    database *db.SqliteStreamDB,
    peersAddrs string,
    joinFlag bool,
) (*dragonboat.NodeHost, error) {

    logger.GetLogger("raft").SetLevel(logger.ERROR)
    logger.GetLogger("rsm").SetLevel(logger.WARNING)
    logger.GetLogger("transport").SetLevel(logger.ERROR)
    logger.GetLogger("grpc").SetLevel(logger.WARNING)

    cfg := config.Config{
        NodeID:                  nodeID,
        ClusterID:               clusterID,
        ElectionRTT:             10,
        HeartbeatRTT:            1,
        CheckQuorum:             true,
        SnapshotEntries:         100_000,
        CompactionOverhead:      0,
        EntryCompressionType:    config.Snappy,
        SnapshotCompressionType: config.Snappy,
    }

    metaAbsPath := fmt.Sprintf("%s/node-%d", metaPath, nodeID)
    hostConfig := config.NodeHostConfig{
        WALDir:         metaAbsPath,
        NodeHostDir:    metaAbsPath,
        RTTMillisecond: 300,
        RaftAddress:    bindAddress,
    }

    nodeHost, err := dragonboat.NewNodeHost(hostConfig)
    if err != nil {
        return nil, err
    }

    peersMap := parsePeerMap(peersAddrs, bindAddress, nodeID)
    createStateMachine := func(shardID uint64, replicaID uint64) statemachine.IStateMachine {
        return db.NewDBStateMachine(shardID, replicaID, database.Database)
    }

    err = nodeHost.StartCluster(peersMap, joinFlag, createStateMachine, cfg)
    if err != nil {
        return nil, err
    }

    return nodeHost, nil
}

func parsePeerMap(peersAddrs string, bindAddress string, selfNodeId uint64) map[uint64]string {
    peersMap := make(map[uint64]string)
    peersMap[selfNodeId] = bindAddress
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

    return peersMap
}
