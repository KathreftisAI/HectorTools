package main

import (
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"fmt"
	"os"
	"github.com/Unotechsoftware/HectorTools/config"
	"github.com/Unotechsoftware/HectorTools/tools/HectorAppJSON"
	"github.com/Unotechsoftware/HectorTools/tools/CassandraQuery"
	"github.com/Unotechsoftware/HectorTools/tools/AvroSchema"
	"github.com/Unotechsoftware/HectorTools/tools/HiveSchema"
)




func main()  {

	config.LoadConfiguration()

	config.CheckConfiguration()


	err := dbm.StartSchemaMapper(dbm.CassandraConfig{
		Keyspace: config.ConfFile.Keyspace,
		Username: config.ConfFile.Username,
		Password: config.ConfFile.Password,
		Host: config.ConfFile.Host})

	if err != nil {
		fmt.Sprintf("Error occured in creating DB connection to Cassandra: %v\n",err)
		os.Exit(1)
	}

	AvroSchema.AvroInit(config.ConfFile.Keyspace,config.ConfFile.Username,config.ConfFile.Password,config.ConfFile.Host,config.ConfFile.HiveDBName,config.ConfFile.AvroSchemaPath)

	HiveSchema.HiveInit(config.ConfFile.Keyspace,config.ConfFile.Username,config.ConfFile.Password,config.ConfFile.Host,config.ConfFile.HiveDBName,config.ConfFile.HiveSchemaPath)

	CassandraQuery.CassQueryInit(config.ConfFile.Keyspace,config.ConfFile.Username,config.ConfFile.Password,config.ConfFile.Host,config.ConfFile.CassQueryPath)

	HectorAppJSON.MakeApi()

	err = HectorAppJSON.WriteAPIJson()
	if err != nil{
		fmt.Sprintf("Error writing App JSON file: %v \n",err.Error())
	}

	CassandraQuery.MakeCassQuery()

	err = CassandraQuery.WriteCassQuery()

	if err != nil{
		fmt.Sprintf("Error writing Cassandra Query file: %v \n",err.Error())
	}

	AvroSchema.MakeAvroSchema()

	err = AvroSchema.WriteAvroSchema()

	if err != nil{
		fmt.Sprintf("Error Writing Avro Schema file: %v \n", err.Error())
	}

	HiveSchema.MakeHiveSchema()

	err = HiveSchema.WriteHiveSchema()

	if err != nil{
		fmt.Sprintf("Error writing the hive schema file: %v \n",err.Error())
	}
}
