//go:build mage
// +build mage

package main

import (
	"fmt"
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
)

// Hello prints a message (shows that you can define custom Mage targets).
func Hello() {
	fmt.Println("hello plugin developer!")
}

// Default configures the default target.
var Default = build.BuildAll

var _ = build.SetBeforeBuildCallback(func(cfg build.Config) (build.Config, error) {
	fmt.Printf("🔧 Building for OS: %s, Architecture: %s\n", cfg.OS, cfg.Arch)
	return cfg, nil
})
