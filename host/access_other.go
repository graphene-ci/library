//go:build !linux

package host

import (
	"errors"
	"io/fs"
)

// setAccess needs openat2: host directories are a Linux machine feature.
// Other platforms only build pipelines (plan, push); they never run it.
func setAccess(string, string, fs.FileMode, int, int) error {
	return errors.New("host directories need a Linux machine")
}
