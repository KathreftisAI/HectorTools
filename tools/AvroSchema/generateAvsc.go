package AvroSchema

import (
	"encoding/json"
	"os"
	"time"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
)

type AvroArray struct{
	AFType string `json:"type,omitempty"`
	AFItems []string `json:"items,omitempty"`
}


type AvroJson struct {
	TypeVal string `json:"type"`
	NameVal string `json:"name"`
	NSVal   string `json:"namespace"`
	FieldsVal []AvroRecord `json:"fields"`
}

type AvroRecord struct {
	AFName string `json:"name,omitempty"`
	AFType []interface{} `json:"type,omitempty"`
	AFDefault string `json:"default,omitempty"`
}

type AvroConfig struct{
	Keyspace string
	Username string
	Password string
	Host []string
	HiveDBName string
	AvroSchemaPath string
}

var AC AvroConfig



func AvroInit(ks string, user string, pwd string, hosts []string, hivedb string,hcpath string){
	AC.Keyspace = ks
	AC.Username = user
	AC.Password = pwd
	AC.Host = hosts
	AC.HiveDBName = hivedb
	AC.AvroSchemaPath = hcpath

	dbm.StartSchemaMapper(dbm.CassandraConfig{Username:AC.Username,Password:AC.Password,Host:AC.Host,Keyspace:AC.Keyspace})
}



var AvroSchemas map[string]string

func MakeAvroSchema(){
	AvroSchemas = make(map[string]string)

	//TODO: Read data from dbm and map it to AVROJSON struct

	for key, value := range dbm.TableArr {
		var AvroTemp AvroJson


		AvroTemp.TypeVal = "record"
		AvroTemp.NameVal = "%v"
		AvroTemp.NSVal = AC.HiveDBName


		//AvroTemp.FieldsVal = make([]AvroRecord,len(value.Columns))


		for _, value1 := range value.Columns {
			var FieldTemp AvroRecord

			FieldTemp.AFName = value1.Column_name
			FieldTemp.AFType = append(FieldTemp.AFType,"null")
			if value1.Datatype.String() == "ListText"{
				var tempArr AvroArray
				tempArr.AFType = "array"
				tempArr.AFItems = make([]string,2)
				tempArr.AFItems[0] = "null"
				tempArr.AFItems[1] = "string"
				FieldTemp.AFType = append(FieldTemp.AFType, tempArr )
			} else if value1.Datatype.String() == "Timestamp" || value1.Datatype.String() == "TimeUUID"{
				FieldTemp.AFType = append(FieldTemp.AFType,"long")
			} else if value1.Datatype.String() == "Int" || value1.Datatype.String() == "Varint" || value1.Datatype.String() == "Bigint" || value1.Datatype.String() == "Decimal" || value1.Datatype.String() == "Double" || value1.Datatype.String() == "Float"{
				FieldTemp.AFType = append(FieldTemp.AFType,"long")
			} else {
				FieldTemp.AFType = append(FieldTemp.AFType,"text")
			}

			AvroTemp.FieldsVal = append(AvroTemp.FieldsVal, FieldTemp)

		}

		bytJs, err := json.Marshal(AvroTemp)
		if err != nil{
			return
		}

		AvroSchemas[key] = string(bytJs)
	}
}

func WriteAvroSchema() (err error){
	for key, value := range AvroSchemas {
		f,err := os.OpenFile(AC.AvroSchemaPath+"/"+time.Now().Format("20060102150405")+"-"+key+".avsc",os.O_CREATE|os.O_RDWR,0664)

		if err != nil{
			return err
		}

		_, err = f.Write([]byte(value))

		if err != nil{
			return err
		}

	}

	return
}
