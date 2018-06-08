package config

import (
	"github.com/spf13/viper"
	"fmt"
	"os"
	"github.com/mitchellh/mapstructure"
)

type Config struct{
	Fc FileConfiguration
}

type FileConfiguration struct{
	Host []string // Required
	Username string // Required
	Password string // Required
	Keyspace string
	AppJSONPath string
	AvroSchemaPath string
	CassQueryPath string
	HiveSchemaPath string
	HiveDBName string
	// Optional
	NumConnectionsPerHost int
	ConnectionTimeout int
	SocketKeepAlive int
	NumberOfQueryRetries int
}

var ConfFileHolder Config
var ConfFile FileConfiguration

func LoadConfiguration(){

	v := viper.New()
	v.SetConfigName("util")
	v.AddConfigPath("/etc/stream")

	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		fmt.Sprintf("couldn't load config: %s \n", err)
		os.Exit(1)
	}

	if err := v.Unmarshal(&ConfFileHolder); err != nil {
		fmt.Sprintf("couldn't read config: %s \n", err)
	}


	ConfFile = ConfFileHolder.Fc
}

func CheckConfiguration() (err error){

	k := make(map[string]interface{})

	err = mapstructure.Decode(ConfFile, &k)
	if err != nil {
		return
	}

	for key, value := range k {

		ok := true

		switch key {
		case "Host":
			if value == "[]" {
				ok = false
			}
		case "Username", "Password", "Keyspace", "AppJSONPath", "AvroSchemaPath", "CassQueryPath", "HiveSchemaPath":
			if len(value.(string)) == 0 {
				ok = false
			}
		}
		if ok == false {
			err = fmt.Errorf("Field %v is mandatory in the configuration", key)
			return
		}
	}

	return
}
