package spdk

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

type EngineTarget struct {
	sync.RWMutex

	Name       string
	VolumeName string
	SpecSize   uint64
	ActualSize uint64

	ctrlrLossTimeout     int
	fastIOFailTimeoutSec int
	ReplicaStatusMap     map[string]*EngineReplicaStatus

	State    types.InstanceState
	ErrorMsg string

	Head        *api.Lvol
	SnapshotMap map[string]*api.Lvol

	IsRestoring           bool
	RestoringSnapshotName string

	isExpanding           bool
	lastExpansionFailedAt string
	lastExpansionError    string

	// UpdateCh should not be protected by the engine lock
	UpdateCh chan interface{}

	log *safelog.SafeLogger
}

func NewEngineTarget(engineTargetName, volumeName string, specSize uint64, engineTargetUpdateCh chan interface{}) *EngineTarget {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineTargetName": engineTargetName,
		"volumeName":       volumeName,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	return &EngineTarget{
		Name:       engineTargetName,
		VolumeName: volumeName,
		SpecSize:   specSize,

		// TODO: support user-defined values
		ctrlrLossTimeout:     replicaCtrlrLossTimeoutSec,
		fastIOFailTimeoutSec: replicaFastIOFailTimeoutSec,

		ReplicaStatusMap: map[string]*EngineReplicaStatus{},

		State: types.InstanceStatePending,

		SnapshotMap: map[string]*api.Lvol{},

		UpdateCh: engineTargetUpdateCh,

		log: safelog.NewSafeLogger(log),
	}
}

func (et *EngineTarget) Create(spdkClient *spdkclient.Client, replicaAddressMap map[string]string, portCount int32, superiorPortAllocator *commonbitmap.Bitmap, salvageRequested bool) (ret *spdkrpc.EngineTarget, err error) {
	et.log.WithFields(logrus.Fields{
		"portCount":         portCount,
		"replicaAddressMap": replicaAddressMap,
		"salvageRequested":  salvageRequested,
	}).Info("Creating engine target")

	requireUpdate := true

	et.Lock()
	defer func() {
		et.Unlock()
		if requireUpdate {
			et.UpdateCh <- nil
		}
	}()

	// podIP, err := commonnet.GetIPForPod()
	// if err != nil {
	// 	return nil, err
	// }

	if et.State != types.InstanceStatePending {
		requireUpdate = false
		return nil, fmt.Errorf("invalid state %s for engine target %s creation", et.State, et.Name)
	}

	if err := et.ValidateReplicaSize(replicaAddressMap); err != nil {
		return nil, errors.Wrapf(err, "failed to validate replica size during engine target creation")
	}

	defer func() {
		if err != nil {
			et.log.WithError(err).Errorf("Failed to create engine target %s", et.Name)
			if et.State != types.InstanceStateError {
				et.State = types.InstanceStateError
			}
			et.ErrorMsg = err.Error()

			ret = et.getWithoutLock()
			err = nil
		} else {
			if et.State != types.InstanceStateError {
				et.ErrorMsg = ""
			}
		}
	}()

	_, err = spdkClient.BdevRaidGet(et.Name, 0)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to get raid bdev %v during engine target creation", et.Name)
	}

	// if salvageRequested {
	// 	et.log.Info("Requesting salvage for engine replicas")
	// 	replicaAddressMap, err = et.filterSalvageCandidates(replicaAddressMap)
	// 	if err != nil {
	// 		return nil, errors.Wrapf(err, "failed to update replica mode to filter salvage candidates")
	// 	}
	// }

	replicaBdevList := []string{}
	for replicaName, replicaAddr := range replicaAddressMap {
		et.ReplicaStatusMap[replicaName] = &EngineReplicaStatus{
			Address: replicaAddr,
		}

		bdevName, err := connectNVMfBdev(spdkClient, replicaName, replicaAddr, et.ctrlrLossTimeout, et.fastIOFailTimeoutSec)
		if err != nil {
			et.log.WithError(err).Warnf("Failed to get bdev from replica %s with address %s during engine target creation, will mark the mode to ERR and continue",
				replicaName, replicaAddr)
			et.ReplicaStatusMap[replicaName].Mode = types.ModeERR
		} else {
			// TODO: Check if a replica is really a RW replica rather than a rebuilding failed replica
			et.ReplicaStatusMap[replicaName].Mode = types.ModeRW
			et.ReplicaStatusMap[replicaName].BdevName = bdevName
			replicaBdevList = append(replicaBdevList, bdevName)
		}
	}

	if errUpdateLogger := et.log.UpdateLogger(logrus.Fields{
		"replicaStatusMap": et.ReplicaStatusMap,
	}); errUpdateLogger != nil {
		et.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine target creation")
	}

	et.checkAndUpdateInfoFromReplicaNoLock()

	et.log.Infof("Connecting all available replicas %+v, then launching raid during engine creation", et.ReplicaStatusMap)
	if _, err := spdkClient.BdevRaidCreate(et.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, ""); err != nil {
		return nil, err
	}

	et.State = types.InstanceStateRunning

	et.log.Info("Created engine target")

	return et.getWithoutLock(), nil
}

func (et *EngineTarget) ValidateReplicaSize(replicaAddressMap map[string]string) error {
	if len(replicaAddressMap) == 0 {
		return fmt.Errorf("no replicas provided for engine target %s", et.Name)
	}

	// Validate the engine & replica sizes before creating the engine
	replicaSizeMap := make(map[string]uint64, len(replicaAddressMap))
	for replicaName, replicaAddr := range replicaAddressMap {
		replicaClient, err := GetServiceClient(replicaAddr)
		if err != nil {
			return err
		}
		replica, err := replicaClient.ReplicaGet(replicaName)
		if err != nil {
			return errors.Wrapf(err, "failed to get replica %v from %v", replicaName, replicaAddr)
		}

		replicaSizeMap[replicaName] = replica.SpecSize
	}

	// check if all replica sizes are the same
	expectedSize := uint64(0)
	for _, replicaSize := range replicaSizeMap {
		if expectedSize == 0 {
			expectedSize = replicaSize
			continue
		}

		if expectedSize != replicaSize {
			return fmt.Errorf("found different replica sizes: %+v", replicaSizeMap)
		}
	}

	if et.SpecSize < expectedSize {
		return fmt.Errorf("engine target spec size %d is smaller than replica size %d", et.SpecSize, expectedSize)
	}

	return nil
}

func (et *EngineTarget) getWithoutLock() (res *spdkrpc.EngineTarget) {
	res = &spdkrpc.EngineTarget{
		Name:              et.Name,
		SpecSize:          et.SpecSize,
		ActualSize:        et.ActualSize,
		ReplicaAddressMap: map[string]string{},
		ReplicaModeMap:    map[string]spdkrpc.ReplicaMode{},
		Snapshots:         map[string]*spdkrpc.Lvol{},
		State:             string(et.State),
		ErrorMsg:          et.ErrorMsg,
	}

	for replicaName, replicaStatus := range et.ReplicaStatusMap {
		res.ReplicaAddressMap[replicaName] = replicaStatus.Address
		res.ReplicaModeMap[replicaName] = types.ReplicaModeToGRPCReplicaMode(replicaStatus.Mode)
	}
	res.Head = api.LvolToProtoLvol(et.Head)
	for snapshotName, snapApiLvol := range et.SnapshotMap {
		res.Snapshots[snapshotName] = api.LvolToProtoLvol(snapApiLvol)
	}

	return res
}

func (et *EngineTarget) checkAndUpdateInfoFromReplicaNoLock() {
	replicaMap := map[string]*api.Replica{}
	replicaAncestorMap := map[string]*api.Lvol{}
	hasBackingImage := false
	hasSnapshot := false

	for replicaName, replicaStatus := range et.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW && replicaStatus.Mode != types.ModeWO {
			if replicaStatus.Mode != types.ModeERR {
				et.log.Warnf("Engine found unexpected mode for replica %s with address %s during info update from replica, mark the mode from %v to ERR and continue info update for other replicas", replicaName, replicaStatus.Address, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
			}
			continue
		}

		// Ensure the replica is not rebuilding
		func() {
			replicaServiceCli, err := GetServiceClient(replicaStatus.Address)
			if err != nil {
				et.log.WithError(err).Errorf("Engine failed to get service client for replica %s with address %s, will skip this replica and continue info update for other replicas", replicaName, replicaStatus.Address)
				return
			}

			defer func() {
				if errClose := replicaServiceCli.Close(); errClose != nil {
					et.log.WithError(errClose).Errorf("Engine failed to close replica %s client with address %s during check and update info from replica", replicaName, replicaStatus.Address)
				}
			}()

			replica, err := replicaServiceCli.ReplicaGet(replicaName)
			if err != nil {
				et.log.WithError(err).Warnf("Engine failed to get replica %s with address %s, mark the mode from %v to ERR", replicaName, replicaStatus.Address, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				return
			}

			if replicaStatus.Mode == types.ModeWO {
				shallowCopyStatus, err := replicaServiceCli.ReplicaRebuildingDstShallowCopyCheck(replicaName)
				if err != nil {
					et.log.WithError(err).Warnf("Engine failed to get rebuilding replica %s shallow copy info, will skip this replica and continue info update for other replicas", replicaName)
					return
				}
				if shallowCopyStatus.TotalState == helpertypes.ShallowCopyStateError || shallowCopyStatus.Error != "" {
					et.log.Errorf("Engine found rebuilding replica %s error %v during info update from replica, will mark the mode from WO to ERR and continue info update for other replicas", replicaName, shallowCopyStatus.Error)
					replicaStatus.Mode = types.ModeERR
				}
				// No need to do anything if `shallowCopyStatus.TotalState == helpertypes.ShallowCopyStateComplete`, engine should leave the rebuilding logic to update its mode
				return
			}

			// The ancestor check sequence: the backing image, then the oldest snapshot, finally head
			if replica.BackingImageName != "" {
				hasBackingImage = true
				backingImage, err := replicaServiceCli.BackingImageGet(replica.BackingImageName, replica.LvsUUID)
				if err != nil {
					et.log.WithError(err).Warnf("Failed to get backing image %s with disk UUID %s from replica %s head parent %s, will mark the mode from %v to ERR and continue info update for other replicas", replica.BackingImageName, replica.LvsUUID, replicaName, replica.Head.Parent, replicaStatus.Mode)
					replicaStatus.Mode = types.ModeERR
					return
				}
				replicaAncestorMap[replicaName] = backingImage.Snapshot
				if len(replica.Snapshots) > 0 {
					hasSnapshot = true
				}
			} else {
				if len(replica.Snapshots) > 0 {
					if hasBackingImage {
						et.log.Warnf("Engine found replica %s does not have a backing image while other replicas have during info update for other replicas", replicaName)
					} else {
						hasSnapshot = true
						for snapshotName, snapApiLvol := range replica.Snapshots {
							if snapApiLvol.Parent == "" {
								replicaAncestorMap[replicaName] = replica.Snapshots[snapshotName]
								break
							}
						}
					}
				} else {
					if hasSnapshot {
						et.log.Warnf("Engine found replica %s does not have a snapshot while other replicas have during info update for other replicas", replicaName)
					} else {
						replicaAncestorMap[replicaName] = replica.Head
					}
				}
			}
			if replicaAncestorMap[replicaName] == nil {
				et.log.Warnf("Engine cannot find replica %s ancestor, will skip this replica and continue info update for other replicas", replicaName)
				return
			}
			replicaMap[replicaName] = replica
		}()
	}

	// If there are multiple candidates, the priority is:
	//  1. the earliest backing image if one replica contains a backing image
	//  2. the earliest snapshot if one replica contains a snapshot
	//  3. the earliest volume head
	candidateReplicaName := ""
	earliestCreationTime := time.Now()
	for replicaName, ancestorApiLvol := range replicaAncestorMap {
		if hasBackingImage {
			if ancestorApiLvol.Name == types.VolumeHead || IsReplicaSnapshotLvol(replicaName, ancestorApiLvol.Name) {
				continue
			}
		} else {
			if hasSnapshot {
				if ancestorApiLvol.Name == types.VolumeHead {
					continue
				}
			} else {
				if ancestorApiLvol.Name != types.VolumeHead {
					continue
				}
			}
		}

		creationTime, err := time.Parse(time.RFC3339, ancestorApiLvol.CreationTime)
		if err != nil {
			et.log.WithError(err).Warnf("Failed to parse replica %s ancestor creation time, will skip this replica and continue info update for other replicas: %+v", replicaName, ancestorApiLvol)
			continue
		}
		if earliestCreationTime.After(creationTime) {
			earliestCreationTime = creationTime
			et.SnapshotMap = replicaMap[replicaName].Snapshots
			et.Head = replicaMap[replicaName].Head
			et.ActualSize = replicaMap[replicaName].ActualSize
			if candidateReplicaName != replicaName {
				if candidateReplicaName != "" {
					candidateReplicaAncestorName := replicaAncestorMap[candidateReplicaName].Name
					currentReplicaAncestorName := ancestorApiLvol.Name
					// The ancestor can be backing image, so we need to extract the backing image name from the lvol name
					// Notice that, the disks are not the same for all the replicas, so their backing image lvol names are not the same.
					if types.IsBackingImageSnapLvolName(candidateReplicaAncestorName) {
						candidateReplicaAncestorName, _, err = ExtractBackingImageAndDiskUUID(candidateReplicaAncestorName)
						if err != nil {
							et.log.WithError(err).Warnf("BUG: ancestor name %v is from backingImage.Snapshot lvol name, it should be a valid backing image lvol name", candidateReplicaAncestorName)
						}
					}
					if types.IsBackingImageSnapLvolName(currentReplicaAncestorName) {
						currentReplicaAncestorName, _, err = ExtractBackingImageAndDiskUUID(currentReplicaAncestorName)
						if err != nil {
							et.log.WithError(err).Warnf("BUG: ancestor name %v is from backingImage.Snapshot lvol name, it should be a valid backing image lvol name", currentReplicaAncestorName)
						}
					}

					if candidateReplicaName != "" && candidateReplicaAncestorName != currentReplicaAncestorName {
						et.log.Warnf("Comparing with replica %s ancestor %s, replica %s has a different and earlier ancestor %s, will update info from this replica", candidateReplicaName, replicaAncestorMap[candidateReplicaName].Name, replicaName, ancestorApiLvol.Name)
					}
				}
				candidateReplicaName = replicaName
			}
		}
	}
}

func (et *EngineTarget) SetErrorState() {
	needUpdate := false

	et.Lock()
	defer func() {
		et.Unlock()

		if needUpdate {
			et.UpdateCh <- nil
		}
	}()

	if et.State != types.InstanceStateStopped && et.State != types.InstanceStateError {
		et.State = types.InstanceStateError
		needUpdate = true
	}
}

func (et *EngineTarget) Get() (res *spdkrpc.EngineTarget) {
	et.RLock()
	defer et.RUnlock()

	return et.getWithoutLock()
}
