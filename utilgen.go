package streamUtilGenerator

import(
	dbm "github.com/dminGod/D30-HectorDA/cass_strategy/cass_schema_mapper"
	"fmt"
	"os"
)

func main(){
	loadConfiguration()
	fmt.Println("meow")
	err := dbm.StartSchemaMapper(Conf.CassNConf)

	if err != nil{
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
