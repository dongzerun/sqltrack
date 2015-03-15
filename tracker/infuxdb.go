package tracker

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/dongzerun/sqltrack/util"
	"github.com/rossdylan/influxdbc"
)

func init() {
	RegisterOus("influxdb", func() OutputSource { return NewInfluxStore() })
}

type InfluxStore struct {
	//influxdb
	addr     string
	user     string
	pwd      string
	dbname   string
	influxdb *influxdbc.InfluxDB
	serial   *influxdbc.Series

	sqls chan *SlowSql
	quit chan bool
	// waitgroup
	wg util.WaitGroupWrapper
}

// database := influxdbc.NewInfluxDB("localhost:8083", "testdb", "username", "password")
// series := influxdbc.NewSeries{"Col1", "Col2"}
// series.AddPoint("Col1 data", "Col2 data")
// err := database.WriteSeries([]influx.Series{*series})

//currently only support hostname:8083 not cluster
//maybe after influxdb release 1.0, try to user cluster influxdb
// Addrs   string `toml:"addrs"`
// 	Iuser   string `toml:"influx_user"`
// 	Ipwd    string `toml:"influx_pwd"`
// 	Idbname string `toml:"influx_db"`

// type SlowSql struct {
// 	ID           uint32
// 	ProductName  string
// 	FromHostname string
// 	Timestamp    int64
// 	Schema string
// 	Table  string
// 	PayLoad      string
// 	Fields       []*message.Field
// 	RowsRead     float64
// 	BytesSent    float64
// 	RowsAffected float64
// 	RowsExamined float64
// 	QueryTime    float64
// 	Explains []*SqlExplain
// 	UseIndex bool
// }
func (is *InfluxStore) InitHelper(g *GlobalConfig) {
	is.addr = g.InfluxDBConfig.Addrs
	is.user = g.InfluxDBConfig.Iuser
	is.pwd = g.InfluxDBConfig.Ipwd
	is.dbname = g.InfluxDBConfig.Idbname

	is.sqls = make(chan *SlowSql, 100)

	log.Println("influx config: ", is.addr, is.user, is.pwd, is.dbname)

	if ok := is.checkValid(); !ok {
		log.Fatalln("influxStore check valid failed!!!", is)
	}
	is.influxdb = influxdbc.NewInfluxDB(is.addr, is.dbname, is.user, is.pwd)
	// replace influxdb time with sql's executed timestamp
	is.serial = influxdbc.NewSeries(
		"ms",
		"id",   //id is a unique for same slow sql
		"host", // host is sql's source executed host
		// "time",          // time is sql's executed timestamp
		"schema",        // schema is database name of current sql
		"table",         // table is sql's table, only first one all sql (include join or subquery sql)
		"sql",           // original sql
		"rowsread",      // rows read by this sql
		"bytessent",     // bytes sent by this sql
		"rowsaffected",  // rows affected by this sql
		"rowsexaminded", // rows examined by this sql
		"slowtime",      // slow time by this sql
		"useindex",      // if this sql use index or scan whole table
		"explain")       // simple sql explain , it's a json array
	if g.Base.Product == "" {
		log.Fatalln("product name must be set and not empty")
	}
	is.serial.Name = g.Base.Product
}

func (is *InfluxStore) checkValid() bool {
	if is.addr == "" || is.user == "" || is.pwd == "" || is.dbname == "" {
		return false
	}
	return true
}

func NewInfluxStore() *InfluxStore {
	return &InfluxStore{}
}

// get slowsql from chan *SlowSql and format
// stored in influxed
func (is *InfluxStore) LoopProcess() {
	for {
		select {
		case sql := <-is.sqls:
			log.Println("reveive sql in LoopProcess")
			is.send(sql)
		case <-is.quit:
			log.Println("quit influxstore loopprocess")
			return
		}
	}
}

// type SlowSql struct {
// 	ID           uint32
// 	ProductName  string
// 	FromHostname string
// 	Timestamp    int64
// 	Schema string
// 	Table  string
// 	PayLoad      string
// 	Fields       []*message.Field
// 	RowsRead     float64
// 	BytesSent    float64
// 	RowsAffected float64
// 	RowsExamined float64
// 	QueryTime    float64
// 	Explains []*SqlExplain
// 	UseIndex bool
// }
// "id",            // id is a unique  table for same slow sql
// "host",          // host is sql's source executed host
// "time",          // time is sql's executed timestamp
// "schema",        // schema is database name of current sql
// "table",         // table is sql's table, only first one all sql (include join or subquery sql)
// "sql",           // original sql
// "rowsread",      // rows read by this sql
// "bytessent",     // bytes sent by this sql
// "rowsaffected",  // rows affected by this sql
// "rowsexaminded", // rows examined by this sql
// "slowtime",      // slow time by this sql
// "useindex",      // if this sql use index or scan whole table
// "explain")       // simple sql explain , it's a json array
// err := database.WriteSeries([]influx.Series{*series})

// [3027925413 10.1.8.203 0 shijiazhuang vehicle_post
// SELECT tag,puid, title, thumb_img, city, license_date, road_haul, price, major_category FROM `shijiazhuang`.`vehicle_post`  force index (user_id_index) WHERE `city`=1100 AND (user_id = 356435537 AND post_at >= 1424973620 AND minor_category = 1212 AND deal_type = 0 AND listing_status = 5 AND puid <> 1418989540)   LIMIT 0, 300;
//  616.00 1646.00 0.00 0.13 true
//  [{"id":"1","select_type":"SIMPLE","table":"vehicle_post","explain_type":"ref","possible_keys":"user_id_index","key":"user_id_index","key_len":"4","ref":"const","rows":"615","extra":"Using where"}]]
func (is *InfluxStore) send(s *SlowSql) {
	is.fillSerial(s)
	if err := is.influxdb.WriteSeries([]influxdbc.Series{*is.serial}); err != nil {
		//just ignore error , continue and to be statsd
		log.Println("write serial error: ", err)
	}
}

func (is *InfluxStore) fillSerial(s *SlowSql) {
	is.serial.Points = make([][]string, 0)

	tmp := []string{
		strconv.FormatUint(uint64(s.ID), 10),
		s.FromHostname,
		//cause Timestamp is nanno second so divided by 1e9
		//1426262304000000000
		// strconv.FormatInt(s., 10),
		s.Schema,
		s.Table,
		s.PayLoad,
		strconv.FormatFloat(s.RowsRead, 'f', 2, 32),
		strconv.FormatFloat(s.BytesSent, 'f', 2, 32),
		strconv.FormatFloat(s.RowsAffected, 'f', 2, 32),
		strconv.FormatFloat(s.RowsExamined, 'f', 2, 32),
		strconv.FormatFloat(s.QueryTime, 'f', 2, 32),
		strconv.FormatBool(s.UseIndex),
	}

	if sb, err := json.Marshal(s.Explains); err != nil {
		tmp = append(tmp, "")
	} else {
		tmp = append(tmp, string(sb))
	}
	log.Println("fillserial: ", tmp)
	is.serial.Points = append(is.serial.Points, tmp)
}

// receive slowsql sented to chan *SlowSql
func (is *InfluxStore) ReceiveMsg(msg interface{}) {
	// slowsql type aassert，if falied ,just drop and log
	if sql, ok := msg.(*SlowSql); ok {
		is.sqls <- sql
	} else {
		log.Println("receive non-slowsql content, just drop: ", ok, msg)
	}
}

func (is *InfluxStore) Clean() {
	// is.influxdb.Close()
	close(is.quit)
	is.wg.Wait()
	log.Println("influxStoreHelper stop ....")
}

func (is *InfluxStore) Stop() <-chan bool {
	return is.quit
}
