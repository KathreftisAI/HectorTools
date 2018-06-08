package config

import (
	"github.com/spf13/viper"
	"fmt"
	"os"
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

func CheckConfiguration(){
	if ConfFile.Host == nil{
		fmt.Sprintf("No hosts defined, Please check your configuration file \n")
		os.Exit(1)
	}

	if ConfFile.Username == ""{
		fmt.Sprintf("No user defined, Please check your configuration file \n")
		os.Exit(1)
	}

	if ConfFile.Password == ""{
		fmt.Sprintf("No password defined, Please check your configuration file \n")
		os.Exit(1)
	}

	if ConfFile.Keyspace == ""{
		fmt.Sprintf("No keyspace defined, Please check your configuration file \n")
		os.Exit(1)
	}

	if ConfFile.AppJSONPath == ""{
		fmt.Sprintf("No Application Json Output file path defined, Please check your configuration file\n")
		os.Exit(1)
	}

	if ConfFile.AvroSchemaPath == ""{
		fmt.Sprintf("No Avro Schema Output file path defined, Please check your configuration file\n")
		os.Exit(1)
	}

	if ConfFile.CassQueryPath == ""{
		fmt.Sprintf("No Cassandra Query Output file path defined, Please check your configuration file\n")
		os.Exit(1)
	}

	if ConfFile.HiveSchemaPath == ""{
		fmt.Sprintf("No Hive Schema Output file defined, Please check your configuration\n")
	}
}
