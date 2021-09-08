package pulsar

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var (
	cacheDir string
)

func init() {
	workingDir, _ := os.Getwd()
	cacheDir = filepath.Join(workingDir, ".cache")
	_ = os.MkdirAll(cacheDir, 0755)
}
func downloadAndCache(downloadUrl string) (string, error) {
	parts := strings.Split(downloadUrl, "/")
	fileName := filepath.Join(cacheDir, parts[len(parts)-1])
	if _, err := os.Stat(fileName); err == nil {
		return fileName, nil
	}
	response, err := http.Get(downloadUrl)
	if err != nil {
		return "", err
	}
	cachedFile, err := os.Create(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to download %s to %s %w", downloadUrl, fileName, err)
	}
	_, err = io.Copy(cachedFile, response.Body)
	if err != nil {
		return "", fmt.Errorf("failed to download %s to %s %w", downloadUrl, fileName, err)
	}
	return fileName, nil
}
