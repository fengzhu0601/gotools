package config

import (
	"os"

	"github.com/spf13/viper"
)

func Load(fileName string, cfg interface{}) error {
	viper := viper.New()
	viper.SetConfigName(fileName)
	paths := []string{"./config", "../config", "../../config"}
	for _, path := range paths {
		if pathExists(path) {
			viper.AddConfigPath(path)
			break
		}
	}
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return viper.Unmarshal(cfg)
}

// 判断所给路径文件/文件夹是否存在
func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
