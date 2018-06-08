package main

import(
	"github.com/iancoleman/strcase"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"fmt"
	"os"
	"encoding/json"
	"bytes"
	"strings"
	"github.com/spf13/viper"
	"time"
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

type AvroArray struct{
	AFType string `json:"type,omitempty"`
	AFItems []string `json:"items,omitempty"`
}

type ApiHeader struct {

	Databasetype string    `json:"databasetype"`
	Version int             `json:"version"`
	Tags []string        `json:"tags"`
	Databasename string       `json:"database"`
	Table string         `json:"table"`
	ApiName string       `json:"apiName"`
	UpdateCondition string `json:"updateCondition"`
	UpdateKeys string     `json:"updateKeys"`
	Field []map[string]Field   `json:"fields"`
}

type Field struct {

	Name string          `json:"name"`
	Type string          `json:"type"`
	Column string     `json:"column"`
	ValueType string   `json:"valueType"`
	IndexType string   `json:"indexType"`
	Tags []string     `json:"tags"`
	IsGetField string  `json:"is_get_field"`
}


var ConfFileHolder Config
var ConfFile FileConfiguration

func loadConfiguration(){

	v := viper.New()
	v.SetConfigName("util")
	v.AddConfigPath("/etc/stream")

	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("couldn't load config: %s", err)
		os.Exit(1)
	}

	if err := v.Unmarshal(&ConfFileHolder); err != nil {
		fmt.Printf("couldn't read config: %s", err)
	}


	ConfFile = ConfFileHolder.Fc
}


func checkConfiguration(){
	if ConfFile.Host == nil{
		fmt.Println("No hosts defined, Please check your configuration file or pass it as -host")
		os.Exit(1)
	}

	if ConfFile.Username == ""{
		fmt.Println("No user defined, Please check your configuration file or pass it as -username")
		os.Exit(1)
	}

	if ConfFile.Password == ""{
		fmt.Println("No password defined, Please check your configuration file or pass it as -password")
		os.Exit(1)
	}

	if ConfFile.Keyspace == ""{
		fmt.Println("No keyspace defined, Please check your configuration file")
		os.Exit(1)
	}

	if ConfFile.AppJSONPath == ""{
		fmt.Println("No Application Json Output file path defined, Please check your configuration file")
		os.Exit(1)
	}

	if ConfFile.AvroSchemaPath == ""{
		fmt.Println("No Avro Schema Output file path defined, Please check your configuration file")
		os.Exit(1)
	}

	if ConfFile.CassQueryPath == ""{
		fmt.Println("No Cassandra Query Output file path defined, Please check your configuration file")
		os.Exit(1)
	}

	if ConfFile.HiveSchemaPath == ""{
		fmt.Println("No Hive Schema Output file defined, Please check your configuration")
	}
}


func main()  {

	loadConfiguration()

	checkConfiguration()


	err := dbm.StartSchemaMapper(dbm.CassandraConfig{
		Keyspace: ConfFile.Keyspace,
		Username: ConfFile.Username,
		Password: ConfFile.Password,
		Host: ConfFile.Host})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}



	MakeApi()

	err = writeAPIJson()
	if err != nil{
		fmt.Println("Error writing App JSON file ",err.Error())
	}

	makeCassQuery()

	err = writeCassQuery()

	if err != nil{
		fmt.Println("Error writing Cassandra Query file",err.Error())
	}

	makeAvroSchema()

	err = writeAvroSchema()

	if err != nil{
		fmt.Println("Error Writing Avro Schema file", err.Error())
	}

	makeHiveSchema()

	err = writeHiveSchema()

	if err != nil{
		fmt.Println("Error writing the hive schema file ",err.Error())
	}
}


var HiveSchemas map[string]string

func makeHiveSchema(){
	HiveSchemas = make(map[string]string)
	for key, value := range dbm.TableArr {
		var tempString string
		tempString = "use "+ConfFile.HiveDBName+"; "
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


func writeHiveSchema() (err error){
	for key, value := range HiveSchemas {
		f,err := os.OpenFile(ConfFile.HiveSchemaPath+"/"+time.Now().Format("20060102150405")+"-"+key+".hql",os.O_CREATE|os.O_RDWR,0664)

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



var AvroSchemas map[string]string

func makeAvroSchema(){
	AvroSchemas = make(map[string]string)

	//TODO: Read data from dbm and map it to AVROJSON struct

	for key, value := range dbm.TableArr {
		var AvroTemp AvroJson


		AvroTemp.TypeVal = "record"
		AvroTemp.NameVal = "%v"
		AvroTemp.NSVal = ConfFile.HiveDBName


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


func writeAvroSchema() (err error){
	for key, value := range AvroSchemas {
		f,err := os.OpenFile(ConfFile.AvroSchemaPath+"/"+time.Now().Format("20060102150405")+"-"+key+".avsc",os.O_CREATE|os.O_RDWR,0664)

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


func writeCassQuery() (err error){

	for key, value := range CassQueries {
		f,err := os.OpenFile(ConfFile.CassQueryPath+"/"+time.Now().Format("20060102150405")+"-"+key+".cql",os.O_CREATE|os.O_RDWR,0664)

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

func makeCassQuery() {
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


func writeAPIJson() (err error){


	va, err := json.Marshal(Apis)
	if err != nil {
		fmt.Println(err)
	}

	k := bytes.Buffer{}

	err = json.Indent(&k, va, "", "  ")
	if err != nil {
		return
	}

	buff := k

	f,err := os.OpenFile(ConfFile.AppJSONPath+"/"+time.Now().Format("20060102150405")+"-application.json",os.O_CREATE|os.O_RDWR,0664)

	if err != nil{
		return
	}

	_, err = f.Write(buff.Bytes())

	if err != nil{
		return
	}

	return
}

var Apis []ApiHeader

func MakeApi() {

	for name, _ := range dbm.TableArr {

		tabName := name

		if strings.HasSuffix(tabName, "_hist") {
			continue
		}

		tab := dbm.TableArr[tabName]

		a := ApiHeader{
			Tags : []string{"last_update", "D30_API", "supports_historical"},
			Databasename : ConfFile.Keyspace,
			Table : tab.Table_name,
			ApiName : strcase.ToCamel(tab.Table_name),
			UpdateKeys: GetUpdateKeys(tab),
			Databasetype: "cassandra",
			Version: 1}

		for _, col := range tab.Columns {

			f := Field{
				Column: col.Column_name,
				IndexType: GetOneIndex(col),
				Type: col.Datatype.String(),
				ValueType: IsSingleMulti(col),
				Name: strcase.ToCamel(col.Column_name),
				Tags: GetRelevantTags(col),
				IsGetField: IsGetField(col),}

			a.Field = append(a.Field, map[string]Field{ col.Column_name : f })
		}

		Apis = append(Apis, a)
	}
}

func GetUpdateKeys(t *dbm.Table) (s string) {

	var as []string

	for _, col := range t.PartitionColumns {
		as = append(as, strcase.ToCamel(col.Column_name))
	}

	s = strings.Join(as, ",")

	return
}

func GetOneIndex(c *dbm.Column) (s string) {

	for _, ind := range c.IndexesAvailable {

		s = ind.String()
		return
	}

	return
}

func IsGetField(c *dbm.Column) (s string) {

	if strings.HasPrefix(c.Column_name, "int_") {
		s = "false"
	} else {
		s = "true"
	}

	return
}

func IsSingleMulti(c *dbm.Column) (s string) {

	s = "single"

	if c.Datatype == dbm.SetText ||
		c.Datatype == dbm.MapTextText || c.Datatype == dbm.ListText {

		s = "multi"
	}

	return
}

func GetRelevantTags(c *dbm.Column) (s []string) {

	if c.ColumnRole == dbm.PartitionKey {
		s = append(s, "primary_key")
		s = append(s, "mandatory_field")
	}

	return
}
