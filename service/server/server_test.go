package main

import (
	"testing"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/stretchr/testify/assert"
)

func TestLoadYaml(t *testing.T) {
	config.InitCfgWithDirectPath("../../config/test.yaml")
	assert.Equal(t, int64(64), config.BlockSize)
	assert.Equal(t, "tmp", config.DBDir)
	assert.Equal(t, 9999, config.Port)
}
