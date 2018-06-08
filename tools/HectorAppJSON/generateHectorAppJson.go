package HectorAppJSON

import (
	"encoding/json"
	"fmt"
	"bytes"
	"os"
	"time"
	"strings"
	"github.com/iancoleman/strcase"
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"github.com/Unotechsoftware/HectorTools/config"
)




type Field struct {

	Name string          `json:"name"`
	Type string          `json:"type"`
	Column string     `json:"column"`
	ValueType string   `json:"valueType"`
	IndexType string   `json:"indexType"`
	Tags []string     `json:"tags"`
	IsGetField string  `json:"is_get_field"`
}



var Apis []ApiHeader

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

func WriteAPIJson() (err error){


	va, err := json.Marshal(Apis)
	if err != nil {
		fmt.Sprintf("Error Occured in Marshalling the API data for json: %v \n ",err)
	}

	k := bytes.Buffer{}

	err = json.Indent(&k, va, "", "  ")
	if err != nil {
		return
	}

	buff := k

	f,err := os.OpenFile(config.ConfFile.AppJSONPath+"/"+time.Now().Format("20060102150405")+"-application.json",os.O_CREATE|os.O_RDWR,0664)

	if err != nil{
		return
	}

	_, err = f.Write(buff.Bytes())

	if err != nil{
		return
	}

	return
}

func MakeApi() {

	for name, _ := range dbm.TableArr {

		tabName := name

		if strings.HasSuffix(tabName, "_hist") {
			continue
		}

		tab := dbm.TableArr[tabName]

		a := ApiHeader{
			Tags : []string{"last_update", "D30_API", "supports_historical"},
			Databasename : config.ConfFile.Keyspace,
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
