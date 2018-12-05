package main

import (
	"encoding/json"
	"flag"
	"fmt"
	protocol "github.com/CanalClient/canal-go/protocol"
	"github.com/golang/protobuf/proto"
	"github.com/iisquare/go-notify/canal"
	"github.com/iisquare/go-notify/hbase"
	"github.com/iisquare/go-notify/kafka"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strconv"
)

type Config struct {
	TableName string
	Canal struct{
		Destinations []string
		Address string
		Port int
		Username string
		Password string
		BatchSize int32
	}
	Kafka struct{
		Address []string
	}
	HBase struct{
		ZkQuorum string `yaml:"zkQuorum"`
		ZkRoot string `yaml:"zkRoot"`
	}
}

func config(filename *string) (*Config, error) {
	content, err := ioutil.ReadFile(*filename)
	if err != nil {
		return nil, err
	}
	config := Config{}
	err = yaml.Unmarshal(content, &config)
	return &config, err
}

func primaryKeys(columns []*protocol.Column) map[string]interface{} {
	keys := make(map[string]interface{})
	for _, col := range columns {
		if col.IsKey {
			keys[col.Name] = col.Value
		}
	}
	return keys
}

func notice(message *protocol.Message) []map[string]interface{} {
	var notice []map[string]interface{}
	for _, entry := range message.Entries {
		item := make(map[string]interface{})
		header := entry.GetHeader()
		item["logfileName"] = header.GetLogfileName()
		item["logfileOffset"] = strconv.FormatInt(header.GetLogfileOffset(),10)
		item["executeTime"] = header.GetExecuteTime()
		entryType := entry.GetEntryType()
		item["entryType"] = entryType
		switch entryType {
		case protocol.EntryType_TRANSACTIONBEGIN:
			item["entryTypeName"] = "TRANSACTIONBEGIN"
			break
		case protocol.EntryType_TRANSACTIONEND:
			item["entryTypeName"] = "TRANSACTIONEND"
			break
		case protocol.EntryType_ROWDATA:
			item["entryTypeName"] = "ROWDATA"
			rowChange := new(protocol.RowChange)
			err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
			if err != nil {
				item["error"] = err.Error()
				break
			}
			if rowChange == nil {
				break
			}
			item["isDdl"] = rowChange.GetIsDdl()
			item["sql"] = rowChange.GetSql()
			eventType := rowChange.GetEventType()
			item["eventType"] = eventType
			item["schemaName"] = header.GetSchemaName()
			item["tableName"] = header.GetTableName()
			var rowDatas []map[string]interface{}
			for _, rowData := range rowChange.GetRowDatas() {
				row := make(map[string]interface{})
				switch eventType {
				case protocol.EventType_DELETE:
					row["eventTypeName"] = "DELETE"
					row["primaryKeys"] = primaryKeys(rowData.BeforeColumns)
					break
				case protocol.EventType_INSERT:
					row["eventTypeName"] = "INSERT"
					row["primaryKeys"] = primaryKeys(rowData.AfterColumns)
					break
				case protocol.EventType_UPDATE:
					row["eventTypeName"] = "UPDATE"
					row["primaryKeys"] = primaryKeys(rowData.AfterColumns)
					break
				default:
					row["eventTypeName"] = "UNKNOWN"
				}
				rowDatas = append(rowDatas, row)
			}
			item["rowDatas"] = rowDatas
			break
		default:
			item["entryTypeName"] = "UNKNOWN"
		}
		notice = append(notice, item)
	}
	return notice
}

func main() {
	filepath := flag.String("c", "conf/notify.yml", "set configuration file")
	flag.Parse()
	config, err := config(filepath)
	if err != nil {
		log.Fatal("load config failed:", err.Error())
		os.Exit(2)
	}
	tableName := config.TableName
	canalClient := &canal.Client{
		Destinations:config.Canal.Destinations,
		Address:config.Canal.Address,
		Port:config.Canal.Port,
		UserName:config.Canal.Username,
		PassWord:config.Canal.Password,
		BatchSize:config.Canal.BatchSize,
	}
	kafkaProducer := &kafka.Producer{
		Address:config.Kafka.Address,
	}
	hbaseWriter := &hbase.Writer{
		ZkQuorum:config.HBase.ZkQuorum,
		ZkRoot:config.HBase.ZkRoot,
	}
	defer func() {
		canalClient.DisConnection()
		kafkaProducer.DisConnection()
		hbaseWriter.DisConnection(true)
		if err := recover(); err != nil {
			log.Error("recover error:", err)
		}
		log.Info("ByeBye!")
	}()
	err = canalClient.Connect()
	if err != nil {
		log.Fatal("connect to cacal failed:", err.Error())
		os.Exit(2)
	}
	err = kafkaProducer.Connect()
	if err != nil {
		log.Fatal("connect to kafka failed:", err.Error())
		os.Exit(2)
	}
	err = hbaseWriter.Connect()
	if err != nil {
		log.Fatal("connect to hbase failed:", err.Error())
		os.Exit(2)
	}
	log.Info("ready to get from canal ", config.Canal.Address, ":", config.Canal.Port, " ...")
	canalClient.OnMessage(func(message *protocol.Message) bool {
		notice, err := json.Marshal(notice(message))
		if err != nil {
			log.Warn("json encode notice failed:", err.Error())
			return false
		}
		msg, err := json.Marshal(message)
		if err != nil {
			log.Warn("json encode message failed:", err.Error())
			return false
		}
		partition, offset, err := kafkaProducer.SendMessage(tableName, string(notice))
		if err != nil {
			log.Warn("send to kafka failed:", err.Error())
			return false
		}
		key := fmt.Sprintf("%s-%d-%d", tableName, partition, offset)
		values := map[string]map[string][]byte{"log": map[string][]byte{
			"topic": []byte(tableName),
			"partition": []byte(fmt.Sprintf("%d", partition)),
			"offset": []byte(fmt.Sprintf("%d", offset)),
			"notice": notice,
			"message": msg,
		}}
		_, err = hbaseWriter.Put(tableName, key, values)
		if err != nil {
			log.Warn("write to hbase failed:", err.Error(), " ", key, " ", string(notice), " ", string(msg))
		}
		return true
	})
}
