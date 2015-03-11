package tracker

import (
	"errors"
	"github.com/dongzerun/sqltrack/input"
	"github.com/dongzerun/sqltrack/message"
	"hash/crc32"
	"regexp"
	"strings"
)

var (
	ifUseWhere, _      = regexp.Compile(`(?i:where)`)
	rmLineDelimiter, _ = regexp.Compile(" |\n|\r\n|\t+|`")
	rmVars, _          = regexp.Compile(`\(([0-9]+,?)+\)|\(('[^',]+',?)+\)|[0-9]+|'[^',]*'`)
	rmSelectFrom, _    = regexp.Compile(`(?i:select.*from)`)
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
	Table  []string
	//完整sql，可能被truncate
	PayLoad      string
	Fields       []*message.Field
	RowsRead     int64
	BytesSent    int64
	RowsAffected int64
	RowsExamined int64
	QueryTime    float64

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
	Table    []string
}

func NewSlowSql(g *input.GlobalConfig, msg *message.Message) *SlowSql {
	s := &SlowSql{
		PayLoad:      msg.GetPayload(),
		FromHostname: msg.GetHostname(),
		ProductName:  g.Base.Product,
		Fields:       msg.Fields,
		UseIndex:     true,
	}

	if err := s.genID(); err != nil {
		return nil
	}
	s.setIfUseIndex()
	if err := s.decodeFields(); err != nil {
		return nil
	}

	return s
}

//判断是否走索引，在slowsql这一层面简单的匹配是否有where条件
func (s *SlowSql) setIfUseIndex() {
	if use := ifUseWhere.MatchString(s.PayLoad); !use {
		s.UseIndex = false
	}
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
			s.RowsRead = f.GetValueInteger()[0]
		case "Bytes_sent":
			s.BytesSent = f.GetValueInteger()[0]
		case "Rows_affected":
			s.RowsAffected = f.GetValueInteger()[0]
		case "Rows_examined":
			s.RowsExamined = f.GetValueInteger()[0]
		case "Query_time":
			s.QueryTime = f.GetValueDouble()[0]
		case "schema":
			s.Schema = f.GetValueString()[0]
		default:
		}
	}
	return nil
}
