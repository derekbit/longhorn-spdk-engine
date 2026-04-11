package spdk

import (
	"context"
	"fmt"
	"strings"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func stubSwitchoverANASync(ef *EngineFrontend, err error) {
	ef.syncRemoteEngineTargetANAStatesFn = func(oldEngineIP, oldEngineName, newEngineIP, newEngineName string) error {
		return err
	}
	ef.setRemoteEngineTargetANAStateFn = func(engineIP, engineName string, anaState NvmeTCPANAState) error {
		return err
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetNvmfSuccess(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP NVMe-oF frontend with successful switchover")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)

	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = "nqn.2014-08.org.nvmexpress:uuid:test-a"
	ef.NvmeTcpFrontend.Nguid = "old-nguid"
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
	ef.syncCurrentNVMeTCPPathLocked()
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.EngineIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))

	expectedNQN := getStableVolumeNQN("vol-a")
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, expectedNQN)

	expectedEndpoint := GetNvmfEndpoint(expectedNQN, "10.0.0.2", 3000)
	c.Assert(ef.Endpoint, Equals, expectedEndpoint)
	c.Assert(ef.ActivePath, Equals, "10.0.0.2:3000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 2)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.2:3000"].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateInaccessible)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after target switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevRunningUsesMultipathConnect(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend uses multipath connect while running")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }

	called := false
	stubSwitchoverANASync(ef, nil)

	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		called = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.Endpoint, Equals, "/dev/longhorn/vol-a")

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev switchover")
	}
}

func (s *TestSuite) TestEngineFrontendGetExportsMultipathState(c *C) {
	fmt.Println("Testing EngineFrontend.Get exports multipath path state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.ActivePath = "10.0.0.1:2000"
	ef.PreferredPath = "10.0.0.2:3000"
	ef.NvmeTCPPathMap = map[string]*NvmeTCPPath{
		"10.0.0.1:2000": {
			Address:    "10.0.0.1:2000",
			TargetIP:   "10.0.0.1",
			TargetPort: 2000,
			EngineName: "engine-a",
			Nqn:        getStableVolumeNQN("vol-a"),
			Nguid:      getStableVolumeNGUID("vol-a"),
			ANAState:   NvmeTCPANAStateOptimized,
		},
		"10.0.0.2:3000": {
			Address:    "10.0.0.2:3000",
			TargetIP:   "10.0.0.2",
			TargetPort: 3000,
			EngineName: "engine-b",
			Nqn:        getStableVolumeNQN("vol-a"),
			Nguid:      getStableVolumeNGUID("vol-a"),
			ANAState:   NvmeTCPANAStateNonOptimized,
		},
	}

	got := ef.Get()
	c.Assert(got.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(got.PreferredPath, Equals, "10.0.0.2:3000")
	c.Assert(len(got.Paths), Equals, 2)
	c.Assert(got.Paths[0].Address, Equals, "10.0.0.1:2000")
	c.Assert(got.Paths[0].AnaState, Equals, string(NvmeTCPANAStateOptimized))
	c.Assert(got.Paths[1].Address, Equals, "10.0.0.2:3000")
	c.Assert(got.Paths[1].AnaState, Equals, string(NvmeTCPANAStateNonOptimized))
}

func (s *TestSuite) TestEngineFrontendSuspendIdempotent(c *C) {
	fmt.Println("Testing EngineFrontend.Suspend is idempotent when already suspended")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateSuspended

	err := ef.Suspend(nil)
	c.Assert(err, IsNil)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateSuspended))
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverLookupByEngineName(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver lookup by engine name")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = "nqn.2014-08.org.nvmexpress:uuid:test-a"
	stubSwitchoverANASync(ef, nil)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.EngineName,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, IsNil)

	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.EngineName, Equals, "engine-b")
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverAmbiguousEngineName(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with ambiguous engine name")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			"ef-a": NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1)),
			"ef-b": NewEngineFrontend("ef-b", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1)),
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          "engine-a",
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverInvalidAddress(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with invalid target address")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          "ef-a",
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.InvalidArgument)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverBlockdevRunning(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver for blockdev frontend while running")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error { return nil }
	stubSwitchoverANASync(ef, nil)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverRejectedDuringRestore(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver is rejected during restore")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverRejectedDuringExpand(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver is rejected during expansion")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.isExpanding = true

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetResolveEngineNameFallback(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP NVMe-oF frontend with engine name fallback")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		return "engine-c", nil
	}
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-c")
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetUsesPathMetadataForRemoteANASync(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget resolves remote ANA sync metadata from path records")

	ef := NewEngineFrontend("ef-a", "", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = ""
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
	ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", "10.0.0.1", ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.Nguid, NvmeTCPANAStateOptimized)
	ef.ActivePath = "10.0.0.1:2000"
	ef.PreferredPath = "10.0.0.1:2000"

	called := false
	ef.syncRemoteEngineTargetANAStatesFn = func(oldEngineIP, oldEngineName, newEngineIP, newEngineName string) error {
		called = true
		c.Assert(oldEngineIP, Equals, "10.0.0.1")
		c.Assert(oldEngineName, Equals, "engine-a")
		c.Assert(newEngineIP, Equals, "10.0.0.2")
		c.Assert(newEngineName, Equals, "engine-b")
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevNoOpWithoutSuspend(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend with no-op switchover without suspend")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"

	resolveCalled := false
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		resolveCalled = true
		return "", fmt.Errorf("should not resolve engine name for no-op switchover")
	}

	err := ef.SwitchOverTarget(nil, "", "10.0.0.1:2000", "10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(resolveCalled, Equals, false)

	select {
	case <-updateCh:
		c.Fatal("did not expect update notification for no-op switchover")
	default:
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevConnectFailurePreservesOriginalState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend preserves original state on connect failure")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning

	oldEngineName := "engine-a"
	oldEngineIP := "10.0.0.1"
	oldTargetIP := "10.0.0.1"
	oldTargetPort := int32(2000)
	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")
	oldEndpoint := "/dev/longhorn/vol-a"

	ef.EngineName = oldEngineName
	ef.EngineIP = oldEngineIP
	ef.NvmeTcpFrontend.TargetIP = oldTargetIP
	ef.NvmeTcpFrontend.TargetPort = oldTargetPort
	ef.NvmeTcpFrontend.Nqn = oldNQN
	ef.NvmeTcpFrontend.Nguid = oldNGUID
	ef.syncCurrentNVMeTCPPathLocked()
	ef.Endpoint = oldEndpoint
	ef.dmDeviceIsBusy = true
	ef.initiator = &initiator.Initiator{
		Endpoint:    oldEndpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: oldNQN},
	}
	stubSwitchoverANASync(ef, nil)

	var callTargets []string
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		callTargets = append(callTargets, transportAddress+":"+transportServiceID)
		return fmt.Errorf("connect failed")
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "connect failed"), Equals, true)

	c.Assert(len(callTargets), Equals, 1)
	c.Assert(callTargets[0], Equals, "10.0.0.2:3000")

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.EngineName, Equals, oldEngineName)
	c.Assert(ef.EngineIP, Equals, oldEngineIP)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, oldTargetPort)
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, oldNQN)
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, oldNGUID)
	c.Assert(ef.Endpoint, Equals, oldEndpoint)
	c.Assert(ef.initiator.NVMeTCPInfo, NotNil)
	c.Assert(ef.initiator.NVMeTCPInfo.SubsystemNQN, Equals, oldNQN)
	c.Assert(ef.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateOptimized)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after connect failure")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevANASyncFailurePreservesOriginalState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend preserves original state on ANA sync failure")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning

	oldEngineName := "engine-a"
	oldEngineIP := "10.0.0.1"
	oldTargetIP := "10.0.0.1"
	oldTargetPort := int32(2000)
	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")
	oldEndpoint := "/dev/longhorn/vol-a"

	ef.EngineName = oldEngineName
	ef.EngineIP = oldEngineIP
	ef.NvmeTcpFrontend.TargetIP = oldTargetIP
	ef.NvmeTcpFrontend.TargetPort = oldTargetPort
	ef.NvmeTcpFrontend.Nqn = oldNQN
	ef.NvmeTcpFrontend.Nguid = oldNGUID
	ef.syncCurrentNVMeTCPPathLocked()
	ef.Endpoint = oldEndpoint
	ef.dmDeviceIsBusy = true
	ef.initiator = &initiator.Initiator{
		Endpoint:    oldEndpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: oldNQN},
	}
	ef.getInitiatorEndpointFn = func() string { return oldEndpoint }
	// Stub: the pre-connect ANA state setting (setRemoteEngineTargetANAStateFn)
	// must succeed so that the multipath connect proceeds, but the post-connect
	// ANA sync (syncRemoteEngineTargetANAStatesFn) will fail.
	ef.setRemoteEngineTargetANAStateFn = func(engineIP, engineName string, anaState NvmeTCPANAState) error {
		return nil
	}
	ef.syncRemoteEngineTargetANAStatesFn = func(oldEngineIP, oldEngineName, newEngineIP, newEngineName string) error {
		return fmt.Errorf("ana sync failed")
	}

	var callTargets []string
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		callTargets = append(callTargets, transportAddress+":"+transportServiceID)
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "ana sync failed"), Equals, true)
	c.Assert(len(callTargets), Equals, 1)
	c.Assert(callTargets[0], Equals, "10.0.0.2:3000")

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.EngineName, Equals, oldEngineName)
	c.Assert(ef.EngineIP, Equals, oldEngineIP)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, oldTargetPort)
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, oldNQN)
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, oldNGUID)
	c.Assert(ef.Endpoint, Equals, oldEndpoint)
	c.Assert(ef.initiator.NVMeTCPInfo, NotNil)
	c.Assert(ef.initiator.NVMeTCPInfo.SubsystemNQN, Equals, oldNQN)
	c.Assert(ef.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateOptimized)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after ANA sync failure")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevCreatesInitiatorForMultipathConnect(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend creates initiator before multipath connect")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	stubSwitchoverANASync(ef, nil)

	connected := false
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		connected = true
		if ef.initiator == nil {
			return fmt.Errorf("initiator was not created")
		}
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(connected, Equals, true)
	c.Assert(ef.initiator, NotNil)
	c.Assert(ef.EngineName, Equals, "engine-b")

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevAlreadyConnectedReloadsInitiatorState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend reloads initiator state when target path is already connected")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.dmDeviceIsBusy = true
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}

	connectCalled := false
	deviceReloaded := false
	endpointReloaded := false
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		connectCalled = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		return fmt.Errorf("nvme connect target failed: already connected")
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		deviceReloaded = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		c.Assert(subsystemNQN, Equals, getStableVolumeNQN("vol-a"))
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		endpointReloaded = true
		c.Assert(dmDeviceIsBusy, Equals, true)
		return nil
	}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, IsNil)
	c.Assert(connectCalled, Equals, true)
	c.Assert(deviceReloaded, Equals, true)
	c.Assert(endpointReloaded, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.EngineIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.Endpoint, Equals, "/dev/longhorn/vol-a")
	c.Assert(ef.ActivePath, Equals, "10.0.0.2:3000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 2)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.2:3000"].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateInaccessible)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev already-connected switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevInProgressGuard(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget in-progress guard for SPDK TCP Blockdev frontend")

	updateCh := make(chan interface{}, 2)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateSuspended
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		enteredCh <- struct{}{}
		<-releaseCh
		return nil
	}

	firstErrCh := make(chan error, 1)
	go func() {
			firstErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	}()

	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for first switchover to enter phase-2")
	}

	// While first switchover is in phase-2, read operations should remain responsive.
	getDone := make(chan struct{}, 1)
	go func() {
		_ = ef.Get()
		getDone <- struct{}{}
	}()
	select {
	case <-getDone:
	case <-time.After(1 * time.Second):
		c.Fatal("Get() blocked while switchover is in progress")
	}

	// Concurrent switchover should be rejected by the in-progress guard.
	err := ef.SwitchOverTarget(nil, "engine-c", "10.0.0.3:3000", "10.0.0.3")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "already in progress"), Equals, true)

	close(releaseCh)
	select {
	case err := <-firstErrCh:
		c.Assert(err, IsNil)
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for first switchover to complete")
	}
}

func (s *TestSuite) TestEngineFrontendDeleteRejectedDuringSwitchOver(c *C) {
	fmt.Println("Testing EngineFrontend.Delete is rejected while switch over is in progress")

	updateCh := make(chan interface{}, 2)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		enteredCh <- struct{}{}
		<-releaseCh
		return nil
	}

	switchErrCh := make(chan error, 1)
	go func() {
			switchErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	}()

	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for switchover to enter phase-2")
	}

	deleteErrCh := make(chan error, 1)
	go func() {
		deleteErrCh <- ef.Delete(nil)
	}()

	select {
	case err := <-deleteErrCh:
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "switching over target"), Equals, true)
	case <-time.After(1 * time.Second):
		c.Fatal("Delete() blocked while switchover is in progress")
	}

	close(releaseCh)
	select {
	case err := <-switchErrCh:
		c.Assert(err, IsNil)
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for switchover to complete")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetRejectedDuringExpand(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget is rejected while expansion is in progress")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.isExpanding = true

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "expansion is in progress"), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetRejectedDuringRestore(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget is rejected while restore is in progress")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "10.0.0.2")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "restore is in progress"), Equals, true)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverEngineNotFound(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with target engine not found")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		return "", ErrSwitchOverTargetEngineNotFound
	}

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.NotFound)
}

func (s *TestSuite) TestCreateUblkFrontendNilReturnsCorrectErrorField(c *C) {
	fmt.Println("Testing createUblkFrontend with nil UblkFrontend returns error referencing UblkFrontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendUBLK, 1024, 0, 0, make(chan interface{}, 1))
	ef.UblkFrontend = nil // force nil

	err := ef.createUblkFrontend(nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "UblkFrontend"), Equals, true)
	// Ensure it does NOT reference the wrong field
	c.Assert(strings.Contains(err.Error(), "NvmeTcpFrontend"), Equals, false)
}

func (s *TestSuite) TestPromoteNVMeTCPPathLockedDemotesOldActivePath(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))

	oldAddress := ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", "10.0.0.1", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateOptimized)
	newAddress := ef.upsertNVMeTCPPathLocked("10.0.0.2", 3000, "engine-b", "10.0.0.2", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateNonOptimized)
	ef.ActivePath = oldAddress
	ef.PreferredPath = oldAddress

	changed := ef.promoteNVMeTCPPathLocked(newAddress)
	c.Assert(changed, Equals, true)
	c.Assert(ef.ActivePath, Equals, newAddress)
	c.Assert(ef.PreferredPath, Equals, oldAddress)
	c.Assert(ef.NvmeTCPPathMap[newAddress].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap[oldAddress].ANAState, Equals, NvmeTCPANAStateInaccessible)
}

func (s *TestSuite) TestRemoveNVMeTCPPathLockedUpdatesSelectors(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))

	firstAddress := ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", "10.0.0.1", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateNonOptimized)
	secondAddress := ef.upsertNVMeTCPPathLocked("10.0.0.2", 3000, "engine-b", "10.0.0.2", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateOptimized)
	ef.ActivePath = secondAddress
	ef.PreferredPath = secondAddress

	ef.removeNVMeTCPPathLocked(secondAddress)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, firstAddress)
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)

	ef.removeNVMeTCPPathLocked(firstAddress)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, "")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 0)
}

func (s *TestSuite) TestEngineFrontendDeleteClearsNVMeTCPPathState(c *C) {
	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.syncCurrentNVMeTCPPathLocked()

	err := ef.Delete(nil)
	c.Assert(err, IsNil)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, "")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 0)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredUblkReturnsTrue(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns true for UBLK frontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendUBLK, 1024, 0, 0, make(chan interface{}, 1))

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, true)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNvmeTcpBlockdevNewEngine(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns true for new NVMe/TCP blockdev engine (port=0)")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, true)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNvmeTcpBlockdevExistingEngine(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns false for existing NVMe/TCP blockdev engine (port!=0)")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetPort = 3000

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, false)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNilNvmeTcpFrontendReturnsError(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns error when NvmeTcpFrontend is nil for non-UBLK frontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend = nil

	_, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid NvmeTcpFrontend"), Equals, true)
}
