package spdk

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	btypes "github.com/longhorn/backupstore/types"
	butil "github.com/longhorn/backupstore/util"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

type Restore struct {
	sync.RWMutex

	spdkClient *SPDKClient
	replica    *Replica

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// The snapshot file that stores the restored data in the end.
	LvolName string

	LastRestored           string
	CurrentRestoringBackup string

	ip             string
	port           int32
	executor       helperutil.Executor
	subsystemNQN   string
	controllerName string
	initiator      *nvme.Initiator

	log logrus.FieldLogger
}

func NewRestore(spdkClient *SPDKClient, lvolName, backupUrl, backupName string, replica *Replica) (*Restore, error) {
	log := logrus.WithFields(logrus.Fields{
		"lvolName":   lvolName,
		"backupUrl":  backupUrl,
		"backupName": backupName,
	})

	executor, err := helperutil.GetExecutorByHostProc(nvme.HostProc)
	if err != nil {
		return nil, err
	}

	return &Restore{
		spdkClient:             spdkClient,
		replica:                replica,
		BackupURL:              backupUrl,
		CurrentRestoringBackup: backupName,
		LvolName:               lvolName,
		ip:                     replica.IP,
		port:                   replica.PortStart,
		executor:               executor,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		log:                    log,
	}, nil
}

func (status *Restore) DeepCopy() *Restore {
	status.RLock()
	defer status.RUnlock()

	return &Restore{
		LvolName:               status.LvolName,
		LastRestored:           status.LastRestored,
		BackupURL:              status.BackupURL,
		CurrentRestoringBackup: status.CurrentRestoringBackup,
		State:                  status.State,
		Error:                  status.Error,
		Progress:               status.Progress,
	}
}

func BackupRestore(backupURL, snapshotLvolName string, concurrentLimit int32, restoreObj *Restore) error {
	backupURL = butil.UnescapeURL(backupURL)

	logrus.WithFields(logrus.Fields{
		"backupURL":        backupURL,
		"snapshotLvolName": snapshotLvolName,
		"concurrentLimit":  concurrentLimit,
	}).Info("Start restoring backup")

	return backupstore.RestoreDeltaBlockBackup(&backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        restoreObj,
		Filename:        snapshotLvolName,
		ConcurrentLimit: int32(concurrentLimit),
	})
}

func (r *Restore) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	r.log.Infof("Exposing snapshot lvol bdev for restore")

	lvolName := r.replica.Name
	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(r.spdkClient, r.replica.LvsName, lvolName, r.ip, r.port, r.executor)
	if err != nil {
		r.log.WithError(err).Errorf("Failed to expose snapshot lvol bdev")
		return nil, "", err
	}
	r.subsystemNQN = subsystemNQN
	r.controllerName = controllerName

	r.log.Infof("Creating NVMe initiator for snapshot lvol bdev")
	initiator, err := nvme.NewInitiator(lvolName, helpertypes.GetNQN(lvolName), nvme.HostProc)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to create NVMe initiator for snapshot lvol bdev %v", lvolName)
	}
	if err := initiator.Start(r.ip, strconv.Itoa(int(r.port))); err != nil {
		return nil, "", errors.Wrapf(err, "failed to start NVMe initiator for snapshot lvol bdev %v", lvolName)
	}
	r.initiator = initiator

	r.log.Infof("Opening NVMe device %v", r.initiator.Endpoint)
	fh, err := os.OpenFile(r.initiator.Endpoint, os.O_RDONLY, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v for snapshot lvol bdev %v", r.initiator.Endpoint, lvolName)
	}

	return fh, r.initiator.Endpoint, err
}

func (r *Restore) CloseVolumeDev(volDev *os.File) error {
	r.log.Infof("Closing nvme device %v", r.initiator.Endpoint)
	if err := volDev.Close(); err != nil {
		return errors.Wrapf(err, "failed to close nvme device %v", r.initiator.Endpoint)
	}

	r.log.Infof("Stopping NVMe initiator")
	if err := r.initiator.Stop(); err != nil {
		return errors.Wrapf(err, "failed to stop NVMe initiator")
	}

	r.log.Infof("Unexposing snapshot lvol bdev")
	lvolName := r.replica.Name
	err := r.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
	if err != nil {
		return errors.Wrapf(err, "failed to unexpose snapshot lvol bdev %v", lvolName)
	}

	return nil
}

func (r *Restore) UpdateRestoreStatus(snapshotLvolName string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	r.LvolName = snapshotLvolName
	r.Progress = progress
	if err != nil {
		if r.Error != "" {
			r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
		} else {
			r.Error = err.Error()
		}
		r.State = btypes.ProgressStateError
		r.CurrentRestoringBackup = ""
	}
}

func (r *Restore) FinishRestore() {
	r.Lock()
	defer r.Unlock()
	if r.State != btypes.ProgressStateError {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}
}

/*
func (status *Restore) StartNewRestore(backupURL, currentRestoringBackup, toLvolName, snapshotDiskName string, validLastRestoredBackup bool) {

}
*/

/*
func (status *RestoreStatus) Revert(previousStatus *RestoreStatus) {
}
*/
