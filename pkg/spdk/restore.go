package spdk

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	btypes "github.com/longhorn/backupstore/types"
	commonNs "github.com/longhorn/go-common-libs/ns"
	commonTypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

type Restore struct {
	sync.RWMutex

	spdkClient *spdkclient.Client
	replica    *Replica

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// The snapshot file that stores the restored data in the end.
	LvolName     string
	SnapshotName string

	LastRestored           string
	CurrentRestoringBackup string

	ip             string
	port           int32
	executor       *commonNs.Executor
	subsystemNQN   string
	controllerName string
	initiator      *nvme.Initiator

	stopOnce sync.Once
	stopChan chan struct{}

	log logrus.FieldLogger
}

func NewRestore(spdkClient *spdkclient.Client, lvolName, snapshotName, backupUrl, backupName string, replica *Replica) (*Restore, error) {
	log := logrus.WithFields(logrus.Fields{
		"lvolName":     lvolName,
		"snapshotName": snapshotName,
		"backupUrl":    backupUrl,
		"backupName":   backupName,
	})

	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create executor")
	}

	return &Restore{
		spdkClient:             spdkClient,
		replica:                replica,
		BackupURL:              backupUrl,
		CurrentRestoringBackup: backupName,
		LvolName:               lvolName,
		SnapshotName:           snapshotName,
		ip:                     replica.IP,
		port:                   replica.PortStart,
		executor:               executor,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		stopChan:               make(chan struct{}),
		log:                    log,
	}, nil
}

func (r *Restore) StartNewRestore(backupUrl, currentRestoringBackup, lvolName, snapshotName string, validLastRestoredBackup bool) {
	r.Lock()
	defer r.Unlock()

	r.LvolName = lvolName
	r.SnapshotName = snapshotName

	r.Progress = 0
	r.Error = ""
	r.BackupURL = backupUrl
	r.State = btypes.ProgressStateInProgress
	if !validLastRestoredBackup {
		r.LastRestored = ""
	}
	r.CurrentRestoringBackup = currentRestoringBackup
}

func (r *Restore) DeepCopy() *Restore {
	r.RLock()
	defer r.RUnlock()

	return &Restore{
		LvolName:               r.LvolName,
		SnapshotName:           r.SnapshotName,
		LastRestored:           r.LastRestored,
		BackupURL:              r.BackupURL,
		CurrentRestoringBackup: r.CurrentRestoringBackup,
		State:                  r.State,
		Error:                  r.Error,
		Progress:               r.Progress,
	}
}

func (r *Restore) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	lvolName := r.replica.Name

	r.log.Info("Unexposing lvol bdev before restoration")
	if r.replica.IsExposed {
		err := r.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to unexpose lvol bdev %v", lvolName)
		}
		r.replica.IsExposed = false
	}

	r.log.Info("Exposing snapshot lvol bdev for restore")
	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(r.spdkClient, r.replica.LvsName, lvolName, r.ip, r.port, r.executor)
	if err != nil {
		r.log.WithError(err).Errorf("Failed to expose lvol bdev")
		return nil, "", err
	}
	r.subsystemNQN = subsystemNQN
	r.controllerName = controllerName
	r.replica.IsExposed = true
	r.log.Infof("Exposed snapshot lvol bdev %v, subsystemNQN=%v, controllerName %v", lvolName, subsystemNQN, controllerName)

	r.log.Info("Creating NVMe initiator for lvol bdev")
	initiator, err := nvme.NewInitiator(lvolName, helpertypes.GetNQN(lvolName), nvme.HostProc)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to create NVMe initiator for lvol bdev %v", lvolName)
	}
	if _, err := initiator.Start(r.ip, strconv.Itoa(int(r.port)), false); err != nil {
		return nil, "", errors.Wrapf(err, "failed to start NVMe initiator for lvol bdev %v", lvolName)
	}
	r.initiator = initiator

	r.log.Infof("Opening NVMe device %v", r.initiator.Endpoint)
	fh, err := os.OpenFile(r.initiator.Endpoint, os.O_RDONLY, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v for lvol bdev %v", r.initiator.Endpoint, lvolName)
	}

	return fh, r.initiator.Endpoint, err
}

func (r *Restore) CloseVolumeDev(volDev *os.File) error {
	r.log.Infof("Closing nvme device %v", r.initiator.Endpoint)
	if err := volDev.Close(); err != nil {
		return errors.Wrapf(err, "failed to close nvme device %v", r.initiator.Endpoint)
	}

	r.log.Info("Stopping NVMe initiator")
	if _, err := r.initiator.Stop(false, false); err != nil {
		return errors.Wrapf(err, "failed to stop NVMe initiator")
	}

	if !r.replica.IsExposeRequired {
		r.log.Info("Unexposing lvol bdev")
		lvolName := r.replica.Name
		err := r.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
		if err != nil {
			return errors.Wrapf(err, "failed to unexpose lvol bdev %v", lvolName)
		}
		r.replica.IsExposed = false
	}

	return nil
}

func (r *Restore) UpdateRestoreStatus(snapshotLvolName string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	r.LvolName = snapshotLvolName
	r.Progress = progress

	if err != nil {
		r.CurrentRestoringBackup = ""

		// No need to mark restore as error if it's cancelled.
		// The restoration will be restarted after the engine is restarted.
		if strings.Contains(err.Error(), btypes.ErrorMsgRestoreCancelled) {
			r.log.WithError(err).Warn("Backup restoration is cancelled")
			r.State = btypes.ProgressStateCanceled
		} else {
			r.log.WithError(err).Error("Backup restoration is failed")
			r.State = btypes.ProgressStateError
			if r.Error != "" {
				r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
			} else {
				r.Error = err.Error()
			}
		}
	}
}

func (r *Restore) FinishRestore() {
	r.Lock()
	defer r.Unlock()

	if r.State != btypes.ProgressStateError && r.State != btypes.ProgressStateCanceled {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}
}

func (r *Restore) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

func (r *Restore) GetStopChan() chan struct{} {
	return r.stopChan
}

// TODL: implement this
// func (status *RestoreStatus) Revert(previousStatus *RestoreStatus) {
// }
