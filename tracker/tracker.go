package tracker

import (
	"github.com/dongzerun/sqltrack/cache"
	"github.com/dongzerun/sqltrack/input"
	"github.com/dongzerun/sqltrack/message"
	"github.com/dongzerun/sqltrack/util"
	"log"
	"strconv"
	"sync"
	"time"
)

// uuid:"\307\305\365\361\234\235G\236\257\372c\005ti1;"
// timestamp:1425885895000000000 type:"mysql.slow-query" l
// ogger:"Sync-1_5-SlowQuery" severity:7
// payload:"SELECT id,minor_category_id,status,post_at FROM `quotation_minor_list` WHERE  status in(2,3);"
// pid:0 hostname:"10.1.8.94"
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

type TrackerStats struct {
	// 处理计数
	ProcessMessageCount    uint64
	ProcessMessageFailures uint64

	// LRU
	ProcessMessageInLru    uint64
	ProcessMessageNotInLru uint64

	//direct sent
	ProcessMessageDirect uint64
}
type Tracker struct {
	g *input.GlobalConfig

	m     sync.Mutex
	mlru  sync.Mutex
	stats *TrackerStats
	// 从kafka处接收并未处理的
	received chan *message.Message
	// 效率优化，可以多开goroutine处理sql并入channel toStore
	toStore chan *SlowSql
	op      *input.OutputSource

	lruPool *cache.LRUCache

	// mysql addr to explain slow sql
	muser  string
	mpwd   string
	maddrs string

	// explainhelper 这个不用接口
	// storehelper 这个需要接口满足插件是开发
	eh *ExplainHelper

	wg   util.WaitGroupWrapper
	quit chan bool
}

// opfactory := input.Ous[globals.Base.Output]()
// var op input.OutputSource

// if op, ok = opfactory.(input.OutputSource); !ok {
// 	log.Fatalln("output may not initiatial!!!")
// }

// log.Println(op)

func NewTracker() *Tracker {
	return &Tracker{
		stats:    &TrackerStats{0, 0, 0, 0, 0},
		received: make(chan *message.Message, 30),
		toStore:  make(chan *SlowSql, 60),
		// lruPool:  cache.NewLRUCache(1024),
	}
}

func (t *Tracker) Init(g *input.GlobalConfig) {
	t.muser = g.Base.Muser
	t.mpwd = g.Base.Mpwd
	t.maddrs = g.Base.Maddrs
	t.g = g
	t.eh = NewExplainHelper(t.muser, t.mpwd, t.maddrs)
	if g.Base.CacheSize > 0 && g.Base.CacheSize <= 1024 {
		t.lruPool = cache.NewLRUCache(g.Base.CacheSize)
		// t.lruPool = cache.NewLRUCache(512)
	} else {
		t.lruPool = cache.NewLRUCache(512)
	}

	//开启多个goroutine同时消费数据
	for i := 0; i < 1; i++ {
		t.wg.Wrap(t.TransferLoop)
	}
	t.wg.Wrap(t.ToSaveStore)
	t.wg.Wrap(t.StatsLoop)
}

func (t *Tracker) ToSaveStore() {
	for {
		select {
		case <-t.toStore:
		case <-t.quit:
			return
		}
	}
}

func (t *Tracker) TransferLoop() {
	for i := 0; i < 30; i++ {
		select {
		case msg := <-t.received:
			// log.Println(msg.GetPayload(), msg.GetTimestamp(), msg.GetFields())
			// t.toStore <- t.transfer(msg)
			s := t.transfer(msg)
			// log.Println(s)
			if s == nil {
				continue
			}
			t.toStore <- s
		case <-t.quit:
			return
		}
	}
}

func (t *Tracker) transfer(msg *message.Message) *SlowSql {
	//NewSlowSql只做预处理，不会去mysql 做 explain
	sql := NewSlowSql(t.g, msg)
	log.Println(sql.Schema, sql.Table, sql.PayLoad)
	//妆步判断，不用走mysql explain，直接打入store channel
	key := strconv.FormatUint(uint64(sql.ID), 10)
	if sql.UseIndex == false && sql.Table != "" {
		t.lruPool.SetIfAbsent(key, sql.GenLruItem())
		t.IcrStatsDirect(1)
		// log.Println("sql direct sented: ", sql.Table, sql.ID, sql.UseIndex, sql.PayLoad)
		return sql
	}

	if v, ok := t.lruPool.Get(key); !ok {
		log.Println("sql not in LruCache: ", sql.Schema, sql.Table, sql.ID, sql.UseIndex, sql.PayLoad)
	} else {
		if it, ok := v.(*LruItem); ok {
			if sql.ID == it.ID {
				sql.UseIndex = it.UseIndex
				sql.Explains = it.Explains
				// sql.Table = it.Table
				// log.Println("sql in LruCache: ", sql.Table, sql.ID, sql.UseIndex, sql.PayLoad)
				t.IcrStatsInLru(1)
				return sql
			}
		}
	}
	t.explainSql(sql)
	t.lruPool.SetIfAbsent(key, sql.GenLruItem())
	// log.Println("sql need explain: ", sql.Table, sql.ID, sql.UseIndex, sql.PayLoad)
	t.IcrStatsNotInLru(1)
	return sql
}

func (t *Tracker) explainSql(sql *SlowSql) {
	// log.Println("explain sql: ", sql.ID, sql.PayLoad)
	// ses := t.eh.Explain(sql)
	// sql.Explains = ses
	// log.Println(sql.PayLoad, "ses is:", ses)
	// for i, _ := range ses {
	// if ses[i].Key == "NULL" || ses[i].ExplainType == "ALL" {
	// sql.UseIndex = false
	// }
	// }
}

func (t *Tracker) Receive(msg *message.Message) {
	t.received <- msg
}

func (t *Tracker) GetAndResetStats() TrackerStats {
	t.m.Lock()
	defer t.m.Unlock()
	ts := TrackerStats{
		t.stats.ProcessMessageCount,
		t.stats.ProcessMessageFailures,
		t.stats.ProcessMessageInLru,
		t.stats.ProcessMessageNotInLru,
		t.stats.ProcessMessageDirect}
	t.stats.ProcessMessageCount = 0
	t.stats.ProcessMessageFailures = 0
	return ts
}

func (t *Tracker) IcrStatsSuccess(delta uint64) {
	t.m.Lock()
	defer t.m.Unlock()
	t.stats.ProcessMessageCount += delta
}

func (t *Tracker) IcrStatsFailure(delta uint64) {
	t.m.Lock()
	defer t.m.Unlock()
	t.stats.ProcessMessageFailures += delta
}

func (t *Tracker) IcrStatsInLru(delta uint64) {
	t.mlru.Lock()
	defer t.mlru.Unlock()
	t.stats.ProcessMessageInLru += delta
}

func (t *Tracker) IcrStatsNotInLru(delta uint64) {
	t.mlru.Lock()
	defer t.mlru.Unlock()
	t.stats.ProcessMessageNotInLru += delta
}

func (t *Tracker) IcrStatsDirect(delta uint64) {
	t.mlru.Lock()
	defer t.mlru.Unlock()
	t.stats.ProcessMessageDirect += delta
}

func (t *Tracker) IcrStatsResetLru() {
	t.mlru.Lock()
	defer t.mlru.Unlock()
	t.stats.ProcessMessageInLru = 0
	t.stats.ProcessMessageNotInLru = 0
	t.stats.ProcessMessageDirect = 0
}

func (t *Tracker) StatsLoop() {
	ticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-ticker.C:
			log.Println("inlru: ", t.stats.ProcessMessageInLru, "notinlru: ", t.stats.ProcessMessageNotInLru,
				"direct: ", t.stats.ProcessMessageDirect, "cachsize:", t.lruPool.StatsJSON())
			t.IcrStatsResetLru()
		case <-t.quit:
			goto exit
		}
	}
exit:
	ticker.Stop()
	return
}
