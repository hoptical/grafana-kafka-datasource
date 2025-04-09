//go:build mage
// +build mage

package main

import (
	"fmt"
	"os"
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
)

// Hello prints a message (shows that you can define custom Mage targets).
func Hello() {
	fmt.Println("hello plugin developer!")
}

var Default = build.BuildAll

var _ = build.SetBeforeBuildCallback(func(cfg build.Config) (build.Config, error) {
	cfg.OS = getEnv("GOOS", "linux")
	cfg.Arch = getEnv("GOARCH", "amd64")

	fmt.Printf("🔧 Building for OS: %s, Architecture: %s\n", cfg.OS, cfg.Arch)

	return cfg, nil
})

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
