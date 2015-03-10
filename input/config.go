package input

import (
	"github.com/bbangert/toml"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type GlobalConfig struct {
	Base        *BaseConfig
	KafkaConfig *KafkaInputConfig
}

type BaseConfig struct {
	Input   string `toml:"input"`
	Output  string `toml:"output"`
	BaseDir string `toml:"basedir"`
	Product string `toml:"product"`

	//runtime
	CpuProf string `toml:"cpuprof"`
	MemProf string `toml:"memprof"`
	MaxCpu  int    `toml:"maxcpu"`

	//mysql
	Muser  string `toml:"mysql_user"`
	Mpwd   string `toml:"mysql_pwd"`
	Maddrs string `toml:"mysql_addrs"`
}

var cfgs map[string]toml.Primitive

func LoadConfig(configPath *string) *GlobalConfig {
	f, err := os.Open(*configPath)
	if err != nil {
		log.Fatalln("open config file or dir failed: ", *configPath)
	}
	defer f.Close()

	fs, err := os.Stat(*configPath)
	if err != nil {
		log.Fatalln("get stat of file or dir failed: ", *configPath)
	}

	if fs.IsDir() {
		log.Fatalln("config file must be file: ", *configPath)
	}

	if !strings.HasSuffix(*configPath, ".toml") {
		log.Fatalln("config file must has .toml suffix")
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("ioutil.ReadAll config file failed: ", *configPath)
	}

	// log.Println(data)

	globals := &GlobalConfig{}
	_, err = toml.Decode(string(data), &cfgs)
	if err != nil {
		log.Fatalln("toml.Decode data failed: ", err)
	}
	log.Println(cfgs)
	// log.Println(cfgs)

	// kafkacfg := &KafkaInputConfig{}
	base := &BaseConfig{}
	kafkacfg := NewKafkaInputConfig()

	empty_ignore := map[string]interface{}{}

	parsed_globals, ok := cfgs["global"]

	if !ok {
		log.Fatalln("global base toml must be set")
	}
	if err = toml.PrimitiveDecodeStrict(parsed_globals, base, empty_ignore); err != nil {
		log.Fatalln("global base decode failed: ", err)
	}

	globals.Base = base

	log.Println(globals.Base)

	parsed_config, ok := cfgs[globals.Base.Input]
	if ok {
		if err = toml.PrimitiveDecodeStrict(parsed_config, kafkacfg, empty_ignore); err != nil {
			// err = fmt.Errorf("Can't unmarshal config: %s", err)
			log.Fatalln("can't unmarshal config: ", err)
		}
		// log.Println(kafkacfg)
		globals.KafkaConfig = kafkacfg
	}

	return globals
}
