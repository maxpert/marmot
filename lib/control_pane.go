package lib

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type ControlPane struct {
	engine *gin.Engine
	raft   *RaftServer
}

func NewControlPane(raft *RaftServer) *ControlPane {
	gin.SetMode(gin.ReleaseMode)
	r := &ControlPane{
		engine: gin.New(),
		raft:   raft,
	}

	r.engine.Use(gin.Recovery())
	routes := r.engine.Group("/", authRequired)
	routes.GET("/start/cluster/:cluster/mode/:mode", r.startCluster)
	routes.GET("/move/cluster/:cluster/node/:node", r.moveCluster)
	routes.GET("/add/cluster/:cluster/node/:node", r.addNode)
	routes.GET("/shuffle", r.shuffleNodes)
	routes.GET("/info", r.clusterInfo)
	routes.GET("/restore", r.restoreSnapshot)

	return r
}

func (c *ControlPane) Run(addr string) error {
	return c.engine.Run(addr)
}

func (c *ControlPane) startCluster(g *gin.Context) {
	clusterId, err := strconv.ParseUint(g.Param("cluster"), 10, 64)
	if err != nil {
		log.Err(err).Msg("bind cluster error")
		g.JSON(500, err.Error())
		return
	}

	members := g.Query("members")
	join := strings.ToLower(g.Param("mode")) == "join"

	err = c.raft.BindCluster(members, join, clusterId)
	if err != nil {
		log.Err(err).Msg("bind cluster error")
		g.JSON(500, err.Error())
		return
	}

	g.JSON(200, true)
}

func (c *ControlPane) addNode(g *gin.Context) {
	nodeId, err := strconv.ParseUint(g.Param("node"), 10, 64)
	if err != nil {
		log.Err(err).Msg("add node error")
		g.JSON(500, err.Error())
		return
	}

	clusterId, err := strconv.ParseUint(g.Param("cluster"), 10, 64)
	if err != nil {
		log.Err(err).Msg("add node error")
		g.JSON(500, err.Error())
		return
	}

	nodeAddrs := g.Query("address")

	err = c.raft.AddNode(nodeId, nodeAddrs, clusterId)
	if err != nil {
		log.Err(err).Msg("add node error")
		g.JSON(500, err.Error())
		return
	}

	g.JSON(200, true)
}

func (c *ControlPane) moveCluster(g *gin.Context) {
	clusterId, err := strconv.ParseUint(g.Param("cluster"), 10, 64)
	if err != nil {
		log.Err(err).Msg("move cluster error")
		g.JSON(500, err.Error())
		return
	}

	nodeId, err := strconv.ParseUint(g.Param("node"), 10, 64)
	if err != nil {
		log.Err(err).Msg("move cluster error")
		g.JSON(500, err.Error())
		return
	}

	err = c.raft.TransferClusters(nodeId, clusterId)
	if err != nil {
		log.Err(err).Msg("move cluster error")
		g.JSON(500, err.Error())
		return
	}

	g.JSON(200, true)
}

func (c *ControlPane) clusterInfo(g *gin.Context) {
	cmap := c.raft.GetNodeMap()
	g.JSON(200, cmap)
}

func (c *ControlPane) restoreSnapshot(g *gin.Context) {
	index, _, err := c.raft.RestoreLatestSnapshot(60 * time.Second)
	if err != nil {
		_ = g.AbortWithError(500, err)
		return
	}

	g.JSON(200, index)
}

func (c *ControlPane) shuffleNodes(g *gin.Context) {
	err := c.raft.ShuffleCluster(c.raft.nodeID)
	if err != nil {
		log.Err(err).Msg("shuffle nodes error")
		g.AbortWithStatus(500)
		return
	}

	// Time to propagate
	time.Sleep(1 * time.Second)
	g.JSON(200, c.raft.GetNodeMap())
}

func authRequired(c *gin.Context) {
	header := c.Request.Header.Get("Authorization")
	payload := os.Getenv("AUTH_KEY")
	if header == "" || header != payload {
		c.Header("WWW-Authenticate", "Basic")
		c.AbortWithStatus(401)
		return
	}
}
