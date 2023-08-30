package util

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func RoundUp(num, base uint64) uint64 {
	if num <= 0 {
		return base
	}
	r := num % base
	if r == 0 {
		return num
	}
	return num - r + base
}

const (
	EngineRandomIDLenth = 8
	EngineSuffix        = "-e"
)

func GetVolumeNameFromEngineName(engineName string) string {
	reg := regexp.MustCompile(fmt.Sprintf(`([^"]*)%s-[A-Za-z0-9]{%d,%d}$`, EngineSuffix, EngineRandomIDLenth, EngineRandomIDLenth))
	return reg.ReplaceAllString(engineName, "${1}")
}

func BytesToMiB(bytes uint64) uint64 {
	return bytes / 1024 / 1024
}

func RemovePrefix(path, prefix string) string {
	if strings.HasPrefix(path, prefix) {
		return strings.TrimPrefix(path, prefix)
	}
	return path
}

func UUID() string {
	return uuid.New().String()
}

func IsSPDKTargetProcessRunning() (bool, error) {
	cmd := exec.Command("pgrep", "-f", "spdk_tgt")
	if _, err := cmd.Output(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if ok {
				exitCode := status.ExitStatus()
				if exitCode == 1 {
					return false, nil
				}
			}
		}
		return false, errors.Wrap(err, "failed to check spdk_tgt process")
	}
	return true, nil
}

func StartSPDKTgtDaemon() error {
	cmd := exec.Command("spdk_tgt")

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start spdk_tgt daemon: %w", err)
	}

	return nil
}

func ParseLabels(labels []string) (map[string]string, error) {
	result := map[string]string{}
	for _, label := range labels {
		kv := strings.SplitN(label, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid label not in <key>=<value> format %v", label)
		}
		key := kv[0]
		value := kv[1]
		if errList := IsQualifiedName(key); len(errList) > 0 {
			return nil, fmt.Errorf("invalid key %v for label: %v", key, errList[0])
		}
		// We don't need to validate the Label value since we're allowing for any form of data to be stored, similar
		// to Kubernetes Annotations. Of course, we should make sure it isn't empty.
		if value == "" {
			return nil, fmt.Errorf("invalid empty value for label with key %v", key)
		}
		result[key] = value
	}
	return result, nil
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputted from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	result = strings.TrimLeft(result, "\"'")
	result = strings.TrimRight(result, "\"'")
	return result
}

func CombineErrors(errorList ...error) (retErr error) {
	for _, err := range errorList {
		if err != nil {
			if retErr != nil {
				retErr = fmt.Errorf("%v, %v", retErr, err)
			} else {
				retErr = err
			}
		}
	}
	return retErr
}

func Min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}
