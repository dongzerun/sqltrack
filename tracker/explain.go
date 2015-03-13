package tracker

import (
	"database/sql"
	// "encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

var (
	EXPLAINERR = errors.New("explain failed!!!")
)

type SqlExplain struct {
	ID           string `json:"id"`
	SelectType   string `json:"select_type"`
	Table        string `json:"table"`
	ExplainType  string `json:"explain_type"`
	PossibleKeys string `json:"possible_keys"`
	Key          string `json:"key"`
	KeyLen       string `json:"key_len"`
	Ref          string `json:"ref"`
	Rows         string `json:"rows"`
	Extra        string `json:"extra"`
}

type ExplainHelper struct {
	//数据库句柄
	dsn string
	DB  *sql.DB
}

func NewExplainHelper(u string, p string, addr string) *ExplainHelper {
	eh := &ExplainHelper{}
	// db, err := sql.Open("mysql", dataSourceName)
	//"root:root@tcp(localhost:3306)/test"
	addrs := strings.Split(addr, ":")
	if len(addrs) == 2 {
		eh.dsn = fmt.Sprintf("%s:%s@tcp(%s)/", u, p, addr)
	} else if len(addrs) == 1 {
		eh.dsn = fmt.Sprintf("%s:%s@tcp(%s:3306)/", u, p, addr)
	} else {
		log.Fatalln("addr decode failed: ", addr)
	}
	var err error
	if eh.DB, err = sql.Open("mysql", eh.dsn); err != nil {
		log.Fatalln("mysql sql.Open err: ", err)
	}

	if err = eh.DB.Ping(); err != nil {
		log.Fatalln("mysql ping failed after established: ", err)
	}

	return eh
}

//返回当前sql的执行执划，如果explain sql执行错误返回空即可
//不强求一定正确
func (e *ExplainHelper) Explain(s *SlowSql) ([]*SqlExplain, error) {

	se := e.explain(s)
	if se != nil {
		return se, nil
	}
	return nil, EXPLAINERR
}

func (e *ExplainHelper) explain(s *SlowSql) []*SqlExplain {
	var err error
	var rows *sql.Rows
	ses := make([]*SqlExplain, 0)
	if s.Schema != "" {
		if _, err = e.DB.Exec(fmt.Sprintf("use %s", s.Schema)); err != nil {
			// just log and continue , bacause some sql execute without use DB
			log.Println("warning set use db:", s.Schema, err)
		}
	}
	esql := fmt.Sprintf("explain %s", s.PayLoad)

	if rows, err = e.DB.Query(esql); err != nil {
		log.Println("query explain sql failed: ", err)
		log.Println("origin wrong sql is: ", s.PayLoad)
		return nil
	}
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Println("get column error: ", err)
		return nil
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		se := &SqlExplain{}
		// 这个写的比较恶心，我受不了了，怎么改比较好呢
		if values[0] == nil {
			se.ID = "NULL"
		} else {
			se.ID = string(values[0])
		}
		if values[1] == nil {
			se.SelectType = "NULL"
		} else {
			se.SelectType = string(values[1])
		}
		if values[2] == nil {
			se.Table = "NULL"
		} else {
			se.Table = string(values[2])
		}
		if values[3] == nil {
			se.ExplainType = "NULL"
		} else {
			se.ExplainType = string(values[3])
		}
		if values[4] == nil {
			se.PossibleKeys = "NULL"
		} else {
			se.PossibleKeys = string(values[4])
		}
		if values[5] == nil {
			se.Key = "NULL"
		} else {
			se.Key = string(values[5])
		}
		if values[6] == nil {
			se.KeyLen = "NULL"
		} else {
			se.KeyLen = string(values[6])
		}
		if values[7] == nil {
			se.Ref = "NULL"
		} else {
			se.Ref = string(values[7])
		}
		if values[8] == nil {
			se.Rows = "NULL"
		} else {
			se.Rows = string(values[8])
		}
		if values[9] == nil {
			se.Extra = "NULL"
		} else {
			se.Extra = string(values[9])
		}
		ses = append(ses, se)
	}
	if err = rows.Err(); err != nil {
		log.Println("rows has err", err)
		return nil
	}
	return ses
}
