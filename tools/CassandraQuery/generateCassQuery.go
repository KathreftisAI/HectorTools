package CassandraQuery

import (
	"os"
	"time"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
)



type CassQueryConfig struct{
	Keyspace string
	Username string
	Password string
	Host []string
	CassQueryPath string
}

var CQC CassQueryConfig


func CassQueryInit(ks string, user string, pwd string, hosts []string, hcpath string){
	CQC.Keyspace = ks
	CQC.Username = user
	CQC.Password = pwd
	CQC.Host = hosts
	CQC.CassQueryPath = hcpath

	dbm.StartSchemaMapper(dbm.CassandraConfig{Username:CQC.Username,Password:CQC.Password,Host:CQC.Host,Keyspace:CQC.Keyspace})
}



func WriteCassQuery() (err error){

	for key, value := range CassQueries {
		f,err := os.OpenFile(CQC.CassQueryPath+"/"+time.Now().Format("20060102150405")+"-"+key+".cql",os.O_CREATE|os.O_RDWR,0664)

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

var CassQueries map[string]string

func MakeCassQuery() {
	CassQueries = make(map[string]string)
	for key, value := range dbm.TableArr {


		var tempData string
		tempData = "SELECT "
		for _, value1 := range value.Columns {
			str1 := value1.Column_name
			if value1.Datatype.String() == "Timestamp"{
				str1 = "blogAsBigInt(timestampAsBlob("+str1+")) as "+str1
			}
			tempData = tempData+str1+", "
		}

		runeVal := []rune(tempData)

		tempData = string(runeVal[:len(runeVal)-1])
		tempData = tempData+" FROM "+value.Keyspace_name+"."+key+" WHERE "+value.PartitionColumns[0].Column_name+" IN (?)"
		CassQueries[key] = tempData
	}
}