package main

import (
	"./service"
	"encoding/json"
	"errors"
	"flag"
	"flamingo/base"
	"flamingo/logger"
	"fmt"
	"github.com/Unknwon/goconfig"
	"os"
	"strconv"
	"strings"
)

var svnVersion = "no provide version"

var (
	addr                   string
	path                   string
	cacheTime              int
	cacheSwitch            bool
	wdjUinUrl              string
	wdjUinNewUrl           string
	pptvUinUrl             string
	wdjUinKey              string
	pptvUinKey             string
	wdjPlatformId          string
	insertLogFile          string
	gpuserMysqlReaderInfo  base.MysqlInfo
	gpuserMysqlWriterInfo  base.MysqlInfo
	gpdevMysqlReaderInfo   base.MysqlInfo
	gpdevMysqlWriterInfo   base.MysqlInfo
	SpecialIdMapArr        [][]uint64
	ThirdProtectAppIdArr   []uint64
	ThirdDeviceAppIdArr    []uint64
	FilterUcidArr          []uint64
	redisInfo              []base.RedisInfo
	userContentInfo        []base.ServerAddr
	blockSecondAppidMapArr [][]uint64
	etcdReceiveUrls        []string
	etcdReportUrls         []string
	etcdDirs               []string
	etcdKey                string
	idc                    string
	optidcs                []string
	stage                  int
	serverType             string
	changLLFirstList       []uint64
)

var (
	BUILD_TIME      = "2017-06-06 06:06:06"
	BUILD_SVN       = "this is svn info"
	BUILD_GOVERSION = "go version"
)

func printVersion() {
	fmt.Println("build time:", BUILD_TIME)
	fmt.Println("build svn info:", BUILD_SVN)
	fmt.Println("build go version:", BUILD_GOVERSION)
}

func initialize() error {
	flag.Parse()
	defer logger.Flush()

	if len(os.Args) < 2 {
		logger.Logln(logger.ERROR, "No conf file")
		return errors.New("No conf file")
	} else {
		if os.Args[len(os.Args)-1] == "version" {
			printVersion()
			return errors.New("get version")
		}
		return loadcfg(os.Args[len(os.Args)-1])
	}
}

func loadcfg(cfile string) error {
	defer logger.Flush()
	var err error
	cfg, err := goconfig.LoadConfigFile(cfile)
	if err != nil {
		logger.Logln(logger.ERROR, "load config failed , errmsg:", err)
		return err
	}
	addr = cfg.MustValue("ThriftInfo", "addr")
	cacheTime = cfg.MustInt("cache", "cache_time")
	cacheSwitch = cfg.MustBool("cache", "cache_switch")

	gpuserMysqlReaderInfo.IP = cfg.MustValue("GPUserMysqlReadInfo", "ip")
	gpuserMysqlReaderInfo.PORT, _ = strconv.Atoi(cfg.MustValue("GPUserMysqlReadInfo", "port"))
	gpuserMysqlReaderInfo.USERNAME = cfg.MustValue("GPUserMysqlReadInfo", "username")
	gpuserMysqlReaderInfo.PASSWORD = cfg.MustValue("GPUserMysqlReadInfo", "password")
	gpuserMysqlReaderInfo.DBNAME = cfg.MustValue("GPUserMysqlReadInfo", "dbname")

	gpuserMysqlWriterInfo.IP = cfg.MustValue("GPUserMysqlWriteInfo", "ip")
	gpuserMysqlWriterInfo.PORT, _ = strconv.Atoi(cfg.MustValue("GPUserMysqlWriteInfo", "port"))
	gpuserMysqlWriterInfo.USERNAME = cfg.MustValue("GPUserMysqlWriteInfo", "username")
	gpuserMysqlWriterInfo.PASSWORD = cfg.MustValue("GPUserMysqlWriteInfo", "password")
	gpuserMysqlWriterInfo.DBNAME = cfg.MustValue("GPUserMysqlWriteInfo", "dbname")

	gpdevMysqlReaderInfo.IP = cfg.MustValue("GPDeveloperMysqlReadInfo", "ip")
	gpdevMysqlReaderInfo.PORT, _ = strconv.Atoi(cfg.MustValue("GPDeveloperMysqlReadInfo", "port"))
	gpdevMysqlReaderInfo.USERNAME = cfg.MustValue("GPDeveloperMysqlReadInfo", "username")
	gpdevMysqlReaderInfo.PASSWORD = cfg.MustValue("GPDeveloperMysqlReadInfo", "password")
	gpdevMysqlReaderInfo.DBNAME = cfg.MustValue("GPDeveloperMysqlReadInfo", "dbname")

	gpdevMysqlWriterInfo.IP = cfg.MustValue("GPDeveloperMysqlWriteInfo", "ip")
	gpdevMysqlWriterInfo.PORT, _ = strconv.Atoi(cfg.MustValue("GPDeveloperMysqlWriteInfo", "port"))
	gpdevMysqlWriterInfo.USERNAME = cfg.MustValue("GPDeveloperMysqlWriteInfo", "username")
	gpdevMysqlWriterInfo.PASSWORD = cfg.MustValue("GPDeveloperMysqlWriteInfo", "password")
	gpdevMysqlWriterInfo.DBNAME = cfg.MustValue("GPDeveloperMysqlWriteInfo", "dbname")

	data_list := cfg.GetKeyList("RedisInfo")
	if len(data_list) > 0 {
		for _, v := range data_list {
			data := cfg.MustValue("RedisInfo", v)
			info := strings.Split(data, ":")
			if len(info) != 6 {
				logger.Logln(logger.ERROR, "redis info error!!", v)
				return nil
			}
			var redis_info base.RedisInfo
			redis_info.DESC = v
			redis_info.IP = info[0]
			redis_info.PORT, _ = strconv.Atoi(info[1])
			redis_info.DB, _ = strconv.Atoi(info[2])
			redis_info.IDLE, _ = strconv.Atoi(info[3])
			redis_info.ACTIVE, _ = strconv.Atoi(info[4])
			redis_info.IDLE_TIMEOUT, _ = strconv.Atoi(info[5])
			redisInfo = append(redisInfo, redis_info)
		}
	}
	logger.Logln(logger.DEBUG, "redis info is ", redisInfo)

	keyList := cfg.GetKeyList("UserContentServerInfo")
	if len(keyList) > 0 {
		for _, key := range keyList {
			data := cfg.MustValue("UserContentServerInfo", key)
			info := strings.Split(data, ":")
			if 3 != len(info) {
				logger.Logln(logger.DEBUG, key, "'s info in config not enoug value", data)
				continue
			}
			var paddr base.ServerAddr
			paddr.IP = info[0]
			paddr.PORT, err = strconv.Atoi(info[1])
			paddr.TIMEOUT, err = strconv.Atoi(info[2])
			userContentInfo = append(userContentInfo, paddr)
		}
	}

	wdjUinUrl = cfg.MustValue("url", "wdj_uin_url")
	wdjUinNewUrl = cfg.MustValue("url", "wdj_new_uin_url")
	pptvUinUrl = cfg.MustValue("url", "pptv_uin_url")
	wdjUinKey = cfg.MustValue("key", "wdj_uin_key")
	pptvUinKey = cfg.MustValue("key", "pptv_uin_key")
	wdjPlatformId = cfg.MustValue("key", "wdj_platform_id")
	insertLogFile = cfg.MustValue("other", "insert_log_file")

	specailAppIdArrJson := cfg.MustValue("other", "app_special_map")
	json.Unmarshal([]byte(specailAppIdArrJson), &SpecialIdMapArr)
	logger.Logln(logger.DEBUG, SpecialIdMapArr)

	blockSecondAppidMapArrJson := cfg.MustValue("other", "block_second_appid_map")
	json.Unmarshal([]byte(blockSecondAppidMapArrJson), &blockSecondAppidMapArr)
	logger.Logln(logger.DEBUG, blockSecondAppidMapArr)

	ThirdProtectAppIdArrJson := cfg.MustValue("other", "thirdProtectAppIdArr")
	json.Unmarshal([]byte(ThirdProtectAppIdArrJson), &ThirdProtectAppIdArr)
	logger.Logln(logger.DEBUG, ThirdProtectAppIdArr)

	ThirdDeviceAppIdArrJson := cfg.MustValue("other", "thirdDeviceAppIdArr")
	json.Unmarshal([]byte(ThirdDeviceAppIdArrJson), &ThirdDeviceAppIdArr)
	logger.Logln(logger.DEBUG, ThirdDeviceAppIdArr)

	filterUcidArrJson := cfg.MustValue("other", "filterUcidArr")
	json.Unmarshal([]byte(filterUcidArrJson), &FilterUcidArr)
	logger.Logln(logger.DEBUG, FilterUcidArr)

	keyList = cfg.GetKeyList("EtcdReceiveUrls")
	if len(keyList) >= 0 {
		for _, key := range keyList {
			etcdReceiveUrls = append(etcdReceiveUrls, cfg.MustValue("EtcdReceiveUrls", key))
		}
	}
	keyList = cfg.GetKeyList("EtcdReportUrls")
	if len(keyList) >= 0 {
		for _, key := range keyList {
			etcdReportUrls = append(etcdReportUrls, cfg.MustValue("EtcdReportUrls", key))
		}
	}
	keyList = cfg.GetKeyList("EtcdDirs")
	if len(keyList) >= 0 {
		for _, key := range keyList {
			etcdDirs = append(etcdDirs, cfg.MustValue("EtcdDirs", key))
		}
	}

	etcdKey = cfg.MustValue("other", "etcd_key")
	idc = cfg.MustValue("other", "idc")
	stage = cfg.MustInt("other", "stage")
	serverType = cfg.MustValue("other", "server_type")
	optIdcsStr := cfg.MustValue("other", "opt_idcs")
	if len(optIdcsStr) > 0 {
		ss := strings.Split(optIdcsStr, ",")
		for _, s := range ss {
			if len(s) > 0 {
				optidcs = append(optidcs, s)
			}
		}
	}

	tmpFirstList := cfg.MustValue("other", "change_ll_first_list")
	if len(tmpFirstList) > 0 {
		ss := strings.Split(tmpFirstList, ",")
		for _, s := range ss {
			if len(s) > 0 {
				cid, err := strconv.Atoi(s)
				if err != nil {
					logger.Logln(logger.ERROR, err)
					continue
				}
				changLLFirstList = append(changLLFirstList, uint64(cid))
			}
		}
	}
	return nil
}

func main() {
	defer logger.Flush()
	fmt.Println("svn version:" + svnVersion)
	if err := initialize(); err != nil {
		return
	}
	var server gp_game_uin.GPGameUinManager
	server.SetServerInfo(addr)

	server.SetCacheTime(cacheTime)
	server.SetCacheSwitch(cacheSwitch)
	server.SetConstUrls(wdjUinUrl, wdjUinNewUrl, pptvUinUrl)
	server.SetConstKeys(wdjUinKey, wdjPlatformId, pptvUinKey)

	server.SetInsertLogFile(insertLogFile)

	server.SetMysqlInfo(gpuserMysqlReaderInfo, gpuserMysqlWriterInfo, gpdevMysqlReaderInfo, gpdevMysqlWriterInfo)

	server.SetSpecailAppIdArr(SpecialIdMapArr)
	server.SetBlockSecondAppIdArr(blockSecondAppidMapArr)
	server.SetThirdProtectAppIdArr(ThirdProtectAppIdArr)
	server.SetThirdDeviceAppIdArr(ThirdDeviceAppIdArr)
	server.SetFilterUcidArr(FilterUcidArr)

	for _, v := range redisInfo {
		server.RedisManager.SetRedisInfo(v)
	}
	server.SetReceiveUrls(etcdReceiveUrls)
	server.SetReportUrls(etcdReportUrls)
	server.SetDirs(etcdDirs)
	server.SetIDC(idc)
	server.SetOptIdcs(optidcs)
	server.SetETCDKey(etcdKey)
	server.SetStage(stage)
	server.SetServerType(serverType)
	server.SetChangLLFirstList(changLLFirstList)
	server.Run()
}
