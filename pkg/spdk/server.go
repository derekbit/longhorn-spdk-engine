package spdk

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util/broadcaster"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

const (
	MonitorInterval = 3 * time.Second

	defaultClusterSize = 1 * 1024 * 1024 // 1MB
	defaultBlockSize   = 4096            // 4KB

	hostPrefix = "/host"
)

type Server struct {
	sync.RWMutex

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *util.Bitmap

	replicaMap map[string]*Replica

	broadcasters map[types.InstanceType]*broadcaster.Broadcaster
	broadcastChs map[types.InstanceType]chan interface{}
	updateChs    map[types.InstanceType]chan interface{}
}

func NewServer(ctx context.Context, portStart, portEnd int32) (*Server, error) {
	cli, err := spdkclient.NewClient()
	if err != nil {
		return nil, err
	}

	broadcasters := map[types.InstanceType]*broadcaster.Broadcaster{}
	broadcastChs := map[types.InstanceType]chan interface{}{}
	updateChs := map[types.InstanceType]chan interface{}{}

	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine} {
		broadcasters[t] = &broadcaster.Broadcaster{}
		broadcastChs[t] = make(chan interface{})
		updateChs[t] = make(chan interface{})
	}

	s := &Server{
		ctx: ctx,

		spdkClient:    cli,
		portAllocator: util.NewBitmap(portStart, portEnd),

		replicaMap: map[string]*Replica{},

		broadcasters: broadcasters,
		broadcastChs: broadcastChs,
		updateChs:    updateChs,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine} {
		if _, err := s.broadcasters[t].Subscribe(ctx, s.broadcastConnector, string(t)); err != nil {
			return nil, err
		}
	}

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoringReplicas()

	return s, nil
}

func (s *Server) monitoringReplicas() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			bdevLvolList, err := s.spdkClient.BdevLvolGet("", 0)
			if err != nil {
				logrus.Errorf("SPDK Server: failed to get lvol bdevs for replica cache update, will retry later: %v", err)
				continue
			}
			bdevLvolMap := map[string]*spdktypes.BdevInfo{}
			for idx := range bdevLvolList {
				bdevLvol := &bdevLvolList[idx]
				if len(bdevLvol.Aliases) == 0 {
					continue
				}
				bdevLvolMap[spdktypes.GetLvolNameFromAlias(bdevLvol.Aliases[0])] = bdevLvol
			}

			s.Lock()
			for _, r := range s.replicaMap {
				r.ValidateAndUpdate(bdevLvolMap)
			}
			s.Unlock()
		}
		if done {
			break
		}
	}
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	s.Lock()
	if s.replicaMap[req.Name] == nil {
		s.replicaMap[req.Name] = NewReplica(req.Name, req.LvsName, req.LvsUuid, req.SpecSize)
	}
	r := s.replicaMap[req.Name]
	s.Unlock()

	portStart, _, err := s.portAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	return r.Create(s.spdkClient, portStart, req.ExposeRequired)
}

func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *empty.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	delete(s.replicaMap, req.Name)
	s.Unlock()

	if r != nil {
		if err := r.Delete(s.spdkClient, req.CleanupRequired); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Error(grpccodes.NotFound, fmt.Sprintf("replica %s is not found during get", req.Name))
	}

	return r.Get()
}

func (s *Server) ReplicaList(ctx context.Context, req *empty.Empty) (*spdkrpc.ReplicaListResponse, error) {
	// TODO: Implement this
	return &spdkrpc.ReplicaListResponse{}, nil
}

func (s *Server) ReplicaWatch(req *empty.Empty, srv spdkrpc.SPDKService_ReplicaWatchServer) error {
	responseCh, err := s.subscribe(types.InstanceTypeReplica)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service update watch errored out")
		} else {
			logrus.Info("SPDK service update watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service update watch")

	for resp := range responseCh {
		r, ok := resp.(*spdkrpc.Replica)
		if !ok {
			return fmt.Errorf("cannot get Replica from channel")
		}
		if err := srv.Send(r); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, fmt.Errorf("replica %s is not found during snapshot create", req.Name)
	}

	return r.SnapshotCreate(s.spdkClient, req.Name)
}

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, fmt.Errorf("replica %s is not found during snapshot delete", req.Name)
	}

	_, err = r.SnapshotDelete(s.spdkClient, req.Name)
	return &empty.Empty{}, err
}

func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	portStart, _, err := s.portAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	return SvcEngineCreate(s.spdkClient, req.Name, req.Frontend, req.ReplicaAddressMap, portStart)
}

func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *empty.Empty, err error) {
	if err = SvcEngineDelete(s.spdkClient, req.Name); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	return SvcEngineGet(s.spdkClient, req.Name)
}

func (s *Server) EngineList(ctx context.Context, req *empty.Empty) (*spdkrpc.EngineListResponse, error) {
	// TODO: Implement this
	return &spdkrpc.EngineListResponse{}, nil
}

func (s *Server) EngineWatch(req *empty.Empty, srv spdkrpc.SPDKService_EngineWatchServer) error {
	responseCh, err := s.subscribe(types.InstanceTypeEngine)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service update watch errored out")
		} else {
			logrus.Info("SPDK service update watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service update watch")

	for resp := range responseCh {
		e, ok := resp.(*spdkrpc.Engine)
		if !ok {
			return fmt.Errorf("cannot get Engine from channel")
		}
		if err := srv.Send(e); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Engine, err error) {
	return SvcEngineSnapshotCreate(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	_, err = SvcEngineSnapshotDelete(s.spdkClient, req.Name, req.SnapshotName)
	return &empty.Empty{}, err
}

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (ret *spdkrpc.Disk, err error) {
	return SvcDiskCreate(s.spdkClient, req.DiskName, req.DiskPath, req.BlockSize)
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	return SvcDiskDelete(s.spdkClient, req.DiskName, req.DiskUuid)
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	return SvcDiskGet(s.spdkClient, req.DiskName, req.DiskPath)
}

func (s *Server) VersionDetailGet(context.Context, *empty.Empty) (*spdkrpc.VersionDetailGetReply, error) {
	// TODO: Implement this
	return &spdkrpc.VersionDetailGetReply{
		Version: &spdkrpc.VersionOutput{},
	}, nil
}

func (s *Server) subscribe(t types.InstanceType) (<-chan interface{}, error) {
	return s.broadcasters[t].Subscribe(context.TODO(), s.broadcastConnector, string(t))
}

func (s *Server) broadcastConnector(t string) (chan interface{}, error) {
	return s.broadcastChs[types.InstanceType(t)], nil
}
