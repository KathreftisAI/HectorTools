package HiveSchema

import (
	"os"
	"time"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"github.com/Unotechsoftware/HectorTools/config"
)


var HiveSchemas map[string]string

func MakeHiveSchema() {
	HiveSchemas = make(map[string]string)
	for key, value := range dbm.TableArr {
		var tempString string
		tempString = "use "+config.ConfFile.HiveDBName+"; "
		//fmt.Println(tempString)
		tempString = tempString + " CREATE EXTERNAL TABLE `hive_"+key+"`( "
		for _,v1 := range value.Columns{
			var tempDT string
			if v1.Datatype.String() == "ListText"{
				tempDT = "array<string>"
			} else if v1.Datatype.String() == "Timestamp" || v1.Datatype.String() == "TimeUUID" || v1.Datatype.String() == "Int" || v1.Datatype.String() == "Varint" || v1.Datatype.String() == "Bigint" || v1.Datatype.String() == "Decimal" || v1.Datatype.String() == "Double" || v1.Datatype.String() == "Float" {
				tempDT = "bigint"
			} else {
				tempDT = "string"
			}
			tempString = tempString + "`"+v1.Column_name+"` "+tempDT+" COMMENT '',"
		}

		runeVal := []rune(tempString)

		tempString = string(runeVal[:len(runeVal)-1])
		tempString = tempString + ") STORED AS AVRO LOCATION '/topics/hive"+key+"';"
		HiveSchemas[key] = tempString
	}
}

func WriteHiveSchema() (err error){
	for key, value := range HiveSchemas {
		f,err := os.OpenFile(config.ConfFile.HiveSchemaPath+"/"+time.Now().Format("20060102150405")+"-"+key+".hql",os.O_CREATE|os.O_RDWR,0664)

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




