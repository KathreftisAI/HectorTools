package streamUtilGenerator

import (
	"flag"
	"github.com/spf13/viper"
	"fmt"
	"os"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
)

type Config struct{
	AvroSchemaPath string
	CassQueryPath string
	HiveTableSchemaPath string
	CassNConf dbm.CassandraConfig
}


var Conf Config

func loadConfiguration(){
	var configFilePath string
	flag.StringVar(&configFilePath,"config_file","/etc/stream/util.toml","Give the path of Configuration file")

	viper.SetConfigFile(configFilePath)
	viper.SetConfigType("toml")
	err := viper.ReadInConfig()

	if err != nil {

		fmt.Println(err.Error())
		os.Exit(1)
	}

	err = viper.Unmarshal(&Conf)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}