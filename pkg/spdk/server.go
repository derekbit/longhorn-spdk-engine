package spdk

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
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
)

type Server struct {
	sync.RWMutex

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *util.Bitmap

	replicaMap map[string]*Replica
	engineMap  map[string]*Engine

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
		engineMap:  map[string]*Engine{},

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
	go s.monitoring()

	return s, nil
}

func (s *Server) monitoring() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			if err := s.verify(); err != nil {
				logrus.WithError(err).Errorf("SPDK Server: failed to verify and update replica cache, will retry later")
			}
		case r := <-s.updateChs[types.InstanceTypeReplica]:
			s.broadcastChs[types.InstanceTypeReplica] <- interface{}(r)
		case e := <-s.updateChs[types.InstanceTypeEngine]:
			s.broadcastChs[types.InstanceTypeEngine] <- interface{}(e)
		}
		if done {
			break
		}
	}
}

func (s *Server) verify() error {
	s.Lock()
	defer s.Unlock()

	bdevList, err := s.spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return err
	}
	subsystemList, err := s.spdkClient.NvmfGetSubsystems("", "")
	if err != nil {
		return err
	}
	bdevMap := map[string]*spdktypes.BdevInfo{}
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		if len(bdev.Aliases) == 1 && spdktypes.GetBdevType(bdev) == spdktypes.BdevTypeLvol {
			lvolName := spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])
			bdevMap[lvolName] = bdev
			bdevLvolMap[lvolName] = bdev
		} else {
			bdevMap[bdev.Name] = bdev
		}
	}
	subsystemMap := map[string]*spdktypes.NvmfSubsystem{}
	for idx := range subsystemList {
		subsystem := &subsystemList[idx]
		subsystemMap[subsystem.Nqn] = subsystem
	}

	for _, r := range s.replicaMap {
		r.ValidateAndUpdate(s.spdkClient, bdevLvolMap, subsystemMap)
	}

	for _, e := range s.engineMap {
		e.ValidateAndUpdate(bdevMap, subsystemMap)
	}

	return nil
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.LvsName == "" || req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs name and lvs UUID are required")
	}

	s.Lock()

	if _, ok := s.replicaMap[req.Name]; ok {
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "replica %v already exists", req.Name)
	}

	s.replicaMap[req.Name] = NewReplica(req.Name, req.LvsName, req.LvsUuid, req.SpecSize)
	r := s.replicaMap[req.Name]

	ret, err = r.Create(s.spdkClient, req.ExposeRequired, s.portAllocator)
	if err != nil {
		return nil, err
	}

	s.Unlock()

	s.updateChs[types.InstanceTypeReplica] <- interface{}(ret)
	return ret, nil
}

func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *empty.Empty, err error) {
	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]
	defer func() {
		if err == nil && req.CleanupRequired {
			delete(s.replicaMap, req.Name)
		}
	}()

	if r != nil {
		if err := r.Delete(s.spdkClient, req.CleanupRequired, s.portAllocator); err != nil {
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
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	return r.Get(), nil
}

func (s *Server) ReplicaList(ctx context.Context, req *empty.Empty) (*spdkrpc.ReplicaListResponse, error) {
	res := map[string]*spdkrpc.Replica{}

	s.RLock()
	for replicaName, r := range s.replicaMap {
		res[replicaName] = r.Get()
	}
	s.RUnlock()

	return &spdkrpc.ReplicaListResponse{Replicas: res}, nil
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
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot create", req.Name)
	}

	return r.SnapshotCreate(s.spdkClient, req.Name)
}

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot delete", req.Name)
	}
	_, err = r.SnapshotDelete(s.spdkClient, req.Name)
	return &empty.Empty{}, err
}

func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" || req.VolumeName == "" {
		return nil, fmt.Errorf("invalid name %s or volume name %s", req.Name, req.VolumeName)
	}
	if req.SpecSize == 0 {
		return nil, fmt.Errorf("invalid spec size %d", req.SpecSize)
	}
	if req.Frontend != types.FrontendSPDKTCPBlockdev && req.Frontend != types.FrontendSPDKTCPNvmf {
		return nil, fmt.Errorf("invalid frontend %s", req.Frontend)
	}

	s.Lock()

	if _, ok := s.engineMap[req.Name]; ok {
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine %v already exists", req.Name)
	}

	s.engineMap[req.Name] = NewEngine(req.Name, req.VolumeName, req.Frontend, req.SpecSize)
	e := s.engineMap[req.Name]

	ret, err = e.Create(s.spdkClient, req.ReplicaAddressMap, s.getLocalReplicaBdevMap(req.ReplicaAddressMap), s.portAllocator)
	if err != nil {
		return nil, err
	}

	s.Unlock()
	s.updateChs[types.InstanceTypeEngine] <- interface{}(ret)
	return ret, nil
}

func (s *Server) getLocalReplicaBdevMap(replicaAddressMap map[string]string) (replicaBdevMap map[string]string) {
	replicaBdevMap = map[string]string{}
	s.Lock()
	for replicaName := range replicaAddressMap {
		r := s.replicaMap[replicaName]
		if r == nil {
			continue
		}
		replicaBdevMap[replicaName] = spdktypes.GetLvolAlias(r.LvsName, r.Name)
	}
	s.Unlock()

	return replicaBdevMap
}

func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *empty.Empty, err error) {
	s.Lock()
	defer s.Unlock()

	e := s.engineMap[req.Name]
	defer func() {
		if err == nil {
			delete(s.engineMap, req.Name)
		}
	}()

	if e != nil {
		if err := e.Delete(s.spdkClient, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v", req.Name)
	}

	return e.Get(), nil
}

func (s *Server) EngineList(ctx context.Context, req *empty.Empty) (*spdkrpc.EngineListResponse, error) {
	res := map[string]*spdkrpc.Engine{}

	s.RLock()
	for engineName, e := range s.engineMap {
		res[engineName] = e.Get()
	}
	s.RUnlock()

	return &spdkrpc.EngineListResponse{Engines: res}, nil
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
	if req.Name == "" || req.SnapshotName == "" {
		return nil, fmt.Errorf("invalid name %s or snapshot name %s", req.Name, req.SnapshotName)
	}

	s.RLock()
	defer s.RUnlock()

	e := s.engineMap[req.Name]

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot creation", req.Name)
	}

	return e.SnapshotCreate(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, fmt.Errorf("invalid name %s or snapshot name %s", req.Name, req.SnapshotName)
	}

	s.RLock()
	defer s.RUnlock()

	e := s.engineMap[req.Name]

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot deletion", req.Name)
	}

	return e.SnapshotDelete(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (ret *spdkrpc.Disk, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskCreate(s.spdkClient, req.DiskName, req.DiskPath, req.BlockSize)
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskDelete(s.spdkClient, req.DiskName, req.DiskUuid)
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskGet(s.spdkClient, req.DiskName, req.DiskPath)
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
