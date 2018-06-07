package main

import(
	"github.com/iancoleman/strcase"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"fmt"
	"os"
	"encoding/json"
	"bytes"
	"strings"
	"flag"
	"github.com/spf13/viper"
)
/*
func main(){
	loadConfiguration()
	fmt.Println("meow")
	err := dbm.StartSchemaMapper(Conf.CassNConf)

	if err != nil{
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
*/

type FileConfiguration struct{
	Host []string // Required
	Username string // Required
	Password string // Required
	Keyspace string

	// Optional
	NumConnectionsPerHost int
	ConnectionTimeout int
	SocketKeepAlive int
	NumberOfQueryRetries int
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


var Conf map[string]interface{}

var ConfFile FileConfiguration

func readFlags(){
	var configFilePath,pwdCass, hostCass, userCass,ksCass,appJsonOut,avScOut,cassQueryOut,hiveScOut string
	flag.StringVar(&configFilePath,"config_file","","Give fully qualified path of configuration file")
	flag.StringVar(&hostCass,"host","","Give the hostname with port for cassandra")
	flag.StringVar(&userCass,"username","","Give the cassandra username")
	flag.StringVar(&pwdCass,"password","","Give the cassandra password")
	flag.StringVar(&ksCass,"keyspace","","Define the Cassandra Keyspace to use")
	flag.StringVar(&appJsonOut,"app_json_out","","Give the output file for storing app json")
	flag.StringVar(&avScOut,"av_sc_out","","Give the output file for storing Avro Schema")
	flag.StringVar(&cassQueryOut,"cass_query_out","","Give the output file for storing Cassandra Query")
	flag.StringVar(&hiveScOut,"hive_sc_out","","Give the output file for storing Hive Schema")

	setConfValue(configFilePath,"config_file")
	setConfValue(hostCass,"cass_host")
	setConfValue(userCass,"cass_user")
	setConfValue(pwdCass,"cass_passwd")
	setConfValue(ksCass,"cass_keyspace")
	setConfValue(appJsonOut,"app_json_out")
	setConfValue(avScOut,"av_sc_out")
	setConfValue(cassQueryOut,"cass_query_out")
	setConfValue(hiveScOut,"hive_sc_out")

}

func setConfValue(value string, key string) {
	if value != ""{
		Conf[key] = value
	}
}

func loadConfiguration(){

	if Conf["cass_host"] == nil || Conf["cass_user"] == nil || Conf["cass_passwd"] == nil || Conf["cass_keyspace"] == nil{
		if Conf["config_file"] == nil{
			Conf["config_file"] = "/etc/stream/util.toml"
		}

		viper.SetConfigFile(Conf["config_file"].(string))
		viper.SetConfigType("toml")
		verr := viper.ReadInConfig()

		e2 := viper.Unmarshal(&ConfFile)

		if e2 != nil {
			fmt.Print("Error marshaling config ", e2)
		}

		if verr != nil {
			fmt.Println("There was an error reading in configuration. Error : ", verr.Error())
		}

		if ConfFile.Host != nil && Conf["cass_host"] == nil{
			Conf["cass_host"] = ConfFile.Host
		}

		if ConfFile.Host != nil && Conf["cass_user"] == nil{
			Conf["cass_user"] = ConfFile.Username
		}

		if ConfFile.Host != nil && Conf["cass_passwd"] == nil{
			Conf["cass_passwd"] = ConfFile.Password
		}

		if ConfFile.Host != nil && Conf["cass_keyspace"] == nil{
			Conf["cass_keyspace"] = ConfFile.Keyspace
		}

	}
}


func checkConfiguration(){
	if Conf["cass_host"] == nil{
		fmt.Println("No hosts defined, Please check your configuration file or pass it as -host")
		os.Exit(1)
	}

	if Conf["cass_user"] == nil{
		fmt.Println("No user defined, Please check your configuration file or pass it as -username")
		os.Exit(1)
	}

	if Conf["cass_passwd"] == nil{
		fmt.Println("No password defined, Please check your configuration file or pass it as -password")
		os.Exit(1)
	}

	if Conf["cass_keyspace"] == nil{
		fmt.Println("No keyspace defined, Please check your configuration file or pass it as -keyspace")
		os.Exit(1)
	}

	if Conf["app_json_out"] == nil{
		fmt.Println("No application json output file defined, Please pass it as -app_json_out")
		os.Exit(1)
	}

	if Conf["av_sc_out"] == nil{
		fmt.Println("No avro schema output file defined, Please pass it as -av_sc_out")
		os.Exit(1)
	}

	if Conf["cass_query_out"] == nil{
		fmt.Println("No Cassandra Query output file defined, Please pass it as -cass_query_out")
		os.Exit(1)
	}

	if Conf["hive_sc_out"] == nil{
		fmt.Println("No Hive Schema Output file defined, Please pass it as -hive_sc_out")
	}
}


func main()  {

	Conf = make(map[string]interface{})

	readFlags()

	loadConfiguration()

	checkConfiguration()

	fmt.Println(Conf["cass_keyspace"].(string)+"ks")
	fmt.Println(Conf["cass_passwd"].(string)+"pass")
	fmt.Println(Conf["cass_user"].(string)+"user")
	fmt.Println(Conf["cass_host"].(string)+"host")


	err := dbm.StartSchemaMapper(dbm.CassandraConfig{
		Keyspace: Conf["cass_keyspace"].(string),
		Username: Conf["cass_user"].(string),
		Password: Conf["cass_passwd"].(string),
		Host: []string{Conf["cass_host"].(string)}})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}



	MakeApi()

	va, err := json.Marshal(Apis)
	if err != nil {
		fmt.Println(err)
	}

	k := bytes.Buffer{}

	err = json.Indent(&k, va, "", "  ")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(k.Bytes()))
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
			Databasename : Conf["cass_keyspace"].(string),
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
