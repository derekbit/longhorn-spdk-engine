package spdk

import (
	"context"
	"errors"
	"fmt"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkjsonrpc "github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBuildBdevLvolMap(c *C) {
	fmt.Println("Testing buildBdevLvolMap with valid lvol, invalid lvol with extra alias, and non-lvol bdev")

	lvolValid := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-valid",
			Aliases:     []string{"lvs-a/replica-a"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{},
		},
	}
	lvolInvalidAlias := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-invalid-alias",
			Aliases:     []string{"lvs-a/replica-b", "extra"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{},
		},
	}
	raid := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "raid-a",
			ProductName: spdktypes.BdevProductNameRaid,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Raid: &spdktypes.BdevRaidInfo{},
		},
	}

	m := buildBdevLvolMap([]spdktypes.BdevInfo{lvolValid, lvolInvalidAlias, raid})
	c.Assert(len(m), Equals, 1)
	c.Assert(m["replica-a"], NotNil)
	c.Assert(m["replica-a"].Name, Equals, "lvol-valid")
}

func (s *TestSuite) TestBuildBdevLvolMapIgnoresInvalidDriverSpecific(c *C) {
	fmt.Println("Testing buildBdevLvolMap ignores invalid driver specific")

	lvolMissingDriverSpecific := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-invalid",
			Aliases:     []string{"lvs-a/replica-a"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: nil,
	}

	m := buildBdevLvolMap([]spdktypes.BdevInfo{lvolMissingDriverSpecific})
	c.Assert(len(m), Equals, 0)
}

func (s *TestSuite) TestBuildLvsUUIDNameMap(c *C) {
	fmt.Println("Testing buildLvsUUIDNameMap with valid lvs list")

	lvsList := []spdktypes.LvstoreInfo{
		{UUID: "uuid-a", Name: "disk-a"},
		{UUID: "uuid-b", Name: "disk-b"},
	}

	m := buildLvsUUIDNameMap(lvsList)
	c.Assert(len(m), Equals, 2)
	c.Assert(m["uuid-a"], Equals, "disk-a")
	c.Assert(m["uuid-b"], Equals, "disk-b")
}

func (s *TestSuite) TestHandleVerifyErrorBrokenPipe(c *C) {
	fmt.Println("Testing handleVerifyError with broken pipe error")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	engine := NewEngine("e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1))
	engineFrontend := NewEngineFrontend("ef1", "e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	replica.State = lhtypes.InstanceStateRunning
	engine.State = lhtypes.InstanceStateRunning
	engineFrontend.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync: map[string]*Engine{
			"e1": engine,
		},
		engineFrontendForSync: map[string]*EngineFrontend{
			"ef1": engineFrontend,
		},
	}

	brokenPipeErr := spdkjsonrpc.JSONClientError{
		ID:          1,
		Method:      "mock",
		ErrorDetail: errors.New("write: broken pipe"),
	}
	server := &Server{}
	server.handleVerifyError(brokenPipeErr, state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engine.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engineFrontend.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestHandleVerifyErrorNonBrokenPipeNoStateChange(c *C) {
	fmt.Println("Testing handleVerifyError with non-broken pipe error does not change state")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replica.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
	}

	server := &Server{}
	server.handleVerifyError(errors.New("any other error"), state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestHandleVerifyErrorNoopForNilError(c *C) {
	fmt.Println("Testing handleVerifyError with nil error does not change state")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replica.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
	}

	server := &Server{}
	server.handleVerifyError(nil, state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestHandleVerifyErrorBrokenPipeKeepsStoppedAndError(c *C) {
	fmt.Println("Testing handleVerifyError with broken pipe error keeps stopped and error states")

	replicaStopped := NewReplica(context.Background(), "r-stopped", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replicaStopped.State = lhtypes.InstanceStateStopped

	engineErrored := NewEngine("e-err", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1))
	engineErrored.State = lhtypes.InstanceStateError

	engineFrontendRunning := NewEngineFrontend("ef-run", "e-err", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	engineFrontendRunning.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r-stopped": replicaStopped,
		},
		engineMapForSync: map[string]*Engine{
			"e-err": engineErrored,
		},
		engineFrontendForSync: map[string]*EngineFrontend{
			"ef-run": engineFrontendRunning,
		},
	}

	brokenPipeErr := spdkjsonrpc.JSONClientError{
		ID:          1,
		Method:      "mock",
		ErrorDetail: errors.New("write: broken pipe"),
	}
	server := &Server{}
	server.handleVerifyError(brokenPipeErr, state)

	c.Assert(replicaStopped.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateStopped))
	c.Assert(engineErrored.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engineFrontendRunning.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestNewVerifyStateLockedCopiesMaps(c *C) {
	fmt.Println("Testing newVerifyState creates copies of maps while locked")

	server := &Server{
		replicaMap: map[string]*Replica{
			"r1": NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1)),
		},
		engineMap: map[string]*Engine{
			"e1": NewEngine("e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1)),
		},
		engineFrontendMap: map[string]*EngineFrontend{
			"ef1": NewEngineFrontend("ef1", "e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1)),
		},
		backingImageMap: map[string]*BackingImage{
			"bi1": NewBackingImage(context.Background(), "bi1", "uuid-bi1", "disk-uuid", 1024, "checksum", make(chan interface{}, 1)),
		},
		spdkClient: nil,
	}

	server.Lock()
	state := server.newVerifyState()
	server.Unlock()

	c.Assert(len(state.replicaMap), Equals, 1)
	c.Assert(len(state.replicaMapForSync), Equals, 1)
	c.Assert(len(state.engineMapForSync), Equals, 1)
	c.Assert(len(state.engineFrontendForSync), Equals, 1)
	c.Assert(len(state.backingImageMap), Equals, 1)
	c.Assert(len(state.backingImageForSync), Equals, 1)
	c.Assert(state.spdkClient, IsNil)

	_, ok := state.replicaMap["r1"]
	c.Assert(ok, Equals, true)
	_, ok = state.engineMapForSync["e1"]
	c.Assert(ok, Equals, true)
	_, ok = state.engineFrontendForSync["ef1"]
	c.Assert(ok, Equals, true)
	_, ok = state.backingImageMap["bi1"]
	c.Assert(ok, Equals, true)
}

func (s *TestSuite) TestSyncVerifiedObjectsWithEmptyState(c *C) {
	fmt.Println("Testing syncVerifiedObjects with empty state")

	server := &Server{}
	state := &verifyState{
		replicaMapForSync:     map[string]*Replica{},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
		backingImageForSync:   map[string]*BackingImage{},
		spdkClient:            nil,
	}

	err := server.syncVerifiedObjects(state)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestEngineFrontendCreateRegistersNewFrontend(c *C) {
	fmt.Println("Testing EngineFrontendCreate registers a new frontend in the map")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngineFrontend: make(chan interface{}, 1),
		},
	}

	_, err := srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-test",
		EngineName: "engine-a",
		VolumeName: "vol-a",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   1024,
	})
	// Create may store the error in ErrorMsg and return nil error.
	_ = err

	srv.RLock()
	ef, ok := srv.engineFrontendMap["ef-test"]
	srv.RUnlock()

	c.Assert(ok, Equals, true)
	c.Assert(ef, NotNil)
	c.Assert(ef.Name, Equals, "ef-test")
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.VolumeName, Equals, "vol-a")
}

func (s *TestSuite) TestEngineFrontendCreateReturnsAlreadyExistsForDuplicate(c *C) {
	fmt.Println("Testing EngineFrontendCreate returns AlreadyExists for duplicate name")

	updateCh := make(chan interface{}, 1)

	existing := NewEngineFrontend("ef-dup", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			"ef-dup": existing,
		},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngineFrontend: updateCh,
		},
	}

	_, err := srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-dup",
		EngineName: "engine-b",
		VolumeName: "vol-b",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   2048,
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.AlreadyExists)

	// Original frontend should be untouched
	srv.RLock()
	ef := srv.engineFrontendMap["ef-dup"]
	srv.RUnlock()
	c.Assert(ef.EngineName, Equals, "engine-a")
}
