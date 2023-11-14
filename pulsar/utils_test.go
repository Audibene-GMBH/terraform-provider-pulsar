package pulsar

import (
	"os"
	"path/filepath"
)

var (
	cacheDir string
)

func init() {
	workingDir, _ := os.Getwd()
	cacheDir = filepath.Join(workingDir, ".cache")
	_ = os.MkdirAll(cacheDir, 0755)
}
