package tracker

import (
	"errors"
	"github.com/dongzerun/sqltrack/input"
	"github.com/dongzerun/sqltrack/message"
	"github.com/dongzerun/sqltrack/util"
	"hash/crc32"
	"log"
	"regexp"
	"strings"
)

var (
	ifUseWhere, _      = regexp.Compile(`(?i:where)`)
	rmLineDelimiter, _ = regexp.Compile(" |\n|\r\n|\t+|`")
	reBackQuote, _     = regexp.Compile("`")
	reSpace, _         = regexp.Compile(`\s+`)
	rmVars, _          = regexp.Compile(`\(([0-9]+,?)+\)|\(('[^',]+',?)+\)|[0-9]+|'[^',]*'`)
	rmSelectFrom, _    = regexp.Compile(`(?i:select.*from)`)
	reSchemaTable1, _  = regexp.Compile(`(?i:( from )(.*)( force | inner |join|leftjoin|rightjoin))`)
	reSchemaTable2, _  = regexp.Compile(`(?i:( from )(.*)( where ))`)
	reSchemaTable3, _  = regexp.Compile(`(?i:( from )(.*)(;))`)
)

var (
	IDERR    = errors.New("general crc32 ID failed or sql is empty!!!")
	FIELDERR = errors.New("decode message.Fields error !!!")
)

type SlowSql struct {
	//抽像后的sql ID
	// crc32算法加密后的
	ID           uint32
	ProductName  string
	FromHostname string
	Timestamp    int64

	//库名，可能为空
	Schema string
	Table  string
	//完整sql，可能被truncate
	PayLoad      string
	Fields       []*message.Field
	RowsRead     float64
	BytesSent    float64
	RowsAffected float64
	RowsExamined float64
	QueryTime    float64

	//explain
	Explains []*SqlExplain

	//未使用索引就是全表扫
	UseIndex bool
}

//为了减少数据库explain压力
//将SlowSql信息进行缓存lru cache中，只缓存ID, UseIndex, Schema, Table
//只要ID相等，就认为命中
type LruItem struct {
	ID       uint32
	UseIndex bool
	Schema   string
	Table    string

	Explains []*SqlExplain
}

func (it *LruItem) Size() int {
	return 1
}

func NewSlowSql(g *input.GlobalConfig, msg *message.Message) *SlowSql {
	s := &SlowSql{
		PayLoad:      msg.GetPayload(),
		FromHostname: msg.GetHostname(),
		ProductName:  g.Base.Product,
		Fields:       msg.Fields,
		UseIndex:     true,
		Explains:     make([]*SqlExplain, 0),
	}

	if err := s.genID(); err != nil {
		log.Println("generate unique id failed: ", err)
		return nil
	}
	s.trySetSchemaAndTable()

	s.setIfUseIndex()
	if err := s.decodeFields(); err != nil {
		log.Println("decode err just reject this slowsql: ", err)
		return nil
	}

	return s
}

//判断是否走索引，在slowsql这一层面简单的匹配是否有where条件
func (s *SlowSql) setIfUseIndex() {
	if use := ifUseWhere.MatchString(s.PayLoad); !use {
		log.Println("ifusewhere false: ", s.ID, s.Table, s.PayLoad)
		s.UseIndex = false
	}
}

func (s *SlowSql) trySetSchemaAndTable() {
	//if match, then m[0],m[1] or m[2]
	// m[1] is schema.table or table
	// 2015/03/11 21:56:24 sql is:  SELECT distinct from_puid, create_at FROM vehicle_offer WHERE from_status = 1 and to_puid = 1415523035 ORDER BY create_at desc LIMIT 0,10;
	// 2015/03/11 21:56:24 0  is  from_puid, create_at FROM vehicle_offer WHERE from_status = 1 and to_puid = 1415523035 ORDER BY create_at desc LIMIT 0,10;
	// 2015/03/11 21:56:24 1  is  _puid, create_at FROM vehicle_offer WHERE from_status = 1 and to_puid = 1415523035 ORDER BY create_at desc LIMIT 0,10
	// 2015/03/11 21:56:24 2  is  ;
	sql := strings.ToLower(s.PayLoad)           //先变小写
	sql = reBackQuote.ReplaceAllString(sql, "") //去掉所有\n \t \r\n分隔符
	sql = reSpace.ReplaceAllString(sql, " ")    //将连续的空格变成一个
	log.Println("old sql is: ", sql)
	m := reSchemaTable1.FindStringSubmatch(sql)
	if len(m) == 4 {
		s.setSchemaAndTable(m[2])
		return
	}

	m = reSchemaTable2.FindStringSubmatch(sql)
	if len(m) == 4 {
		s.setSchemaAndTable(m[2])
		return
	}

	m = reSchemaTable3.FindStringSubmatch(sql)
	if len(m) == 4 {
		s.setSchemaAndTable(m[2])
		return
	}
}

func (s *SlowSql) setSchemaAndTable(m string) {
	//没有schema就用 message的，table为空是可以的
	schema, table := util.SplitToSchemaOrTable(m)
	if schema != "" {
		s.Schema = schema
	}
	s.Table = table
}

func (s *SlowSql) IfUseIndex() bool {
	return s.UseIndex
}

//将sql抽象后生成唯一crc32 ID(会有ID重复问题，不严格一致，忽略即可)
func (s *SlowSql) genID() error {
	sql := strings.ToLower(s.PayLoad)               //先变小写
	sql = rmLineDelimiter.ReplaceAllString(sql, "") //去掉所有\n \t \r\n分隔符
	sql = rmVars.ReplaceAllString(sql, "")          //去掉所有数字和字母变量
	sql = rmSelectFrom.ReplaceAllString(sql, "")
	if sql == "" {
		return IDERR // sql为空，说明这条slow sql异常，上层丢弃即可
	} else {
		s.ID = crc32.ChecksumIEEE([]byte(sql))
		return nil
	}
}

func (s *SlowSql) GenLruItem() *LruItem {
	return &LruItem{
		ID:       s.ID,
		UseIndex: s.UseIndex,
		Schema:   s.Schema,
		Table:    s.Table,
	}
}

// fields:<name:"Rows_read" value_type:DOUBLE value_double:25 >
// fields:<name:"Last_errno" value_string:"0" >
// fields:<name:"Bytes_sent" value_type:DOUBLE value_double:951 >
// fields:<name:"Rows_affected" value_type:DOUBLE value_double:0 >
// fields:<name:"Lock_time" value_type:DOUBLE representation:"s" value_double:3.6e-05 >
// fields:<name:"Rows_examined" value_type:DOUBLE value_double:25 >
// fields:<name:"Query_time" value_type:DOUBLE representation:"s" value_double:0.000161 >
// fields:<name:"Rows_sent" value_type:DOUBLE value_double:24 >
// fields:<name:"Thread_id" value_type:DOUBLE value_double:7.607669555e+09 >
// fields:<name:"Schema" value_string:"fw" >
// fields:<name:"Killed" value_string:"0" >
func (s *SlowSql) decodeFields() error {
	for _, f := range s.Fields {
		switch f.GetName() {
		case "Rows_read":
			if len(f.GetValueDouble()) == 1 {
				s.RowsRead = f.GetValueDouble()[0]
				continue
			}
			return FIELDERR
		case "Bytes_sent":
			if len(f.GetValueDouble()) == 1 {
				s.BytesSent = f.GetValueDouble()[0]
				continue
			}
			return FIELDERR
		case "Rows_affected":
			if len(f.GetValueDouble()) == 1 {
				s.RowsAffected = f.GetValueDouble()[0]
				continue
			}
			return FIELDERR
		case "Rows_examined":
			if len(f.GetValueDouble()) == 1 {
				s.RowsExamined = f.GetValueDouble()[0]
				continue
			}
			return FIELDERR
		case "Query_time":
			if len(f.GetValueDouble()) == 1 {
				s.QueryTime = f.GetValueDouble()[0]
				continue
			}
			return FIELDERR
		case "schema":
			if len(f.GetValueString()) == 1 {
				s.Schema = f.GetValueString()[0]
				continue
			}
			return FIELDERR
		default:
		}
	}
	return nil
}
