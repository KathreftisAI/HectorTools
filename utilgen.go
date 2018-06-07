package main

import(
	"github.com/iancoleman/strcase"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"fmt"
	"os"
	"encoding/json"
	"bytes"
	"strings"
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

func main()  {

	err := dbm.StartSchemaMapper(dbm.CassandraConfig{
		Keyspace: "last_update_test",
		Username: "admin_alltrade",
		Password: "admin_alltrade",
		Host: []string{"10.5.0.8:9042"}})

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
			Databasename : "bss_transformation",
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
