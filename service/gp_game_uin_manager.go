package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"
import (
	"errors"
	"flamingo/base"
	"flamingo/logger"
	"flamingo/utils"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	tbase "go/gen-go/base"
	"strings"
)

type GPGameUinManager struct {
	worker        *GPGameUinWorker
	addr          string
	wdjUinKey     string
	pptvUinKey    string
	wdjPlatformId string
	wdjUinUrl     string
	wdjUinNewUrl  string
	pptvUinUrl    string

	gpuserMysqlReaderInfo base.MysqlInfo
	gpuserMysqlWriterInfo base.MysqlInfo
	gpdevMysqlReaderInfo  base.MysqlInfo
	gpdevMysqlWriterInfo  base.MysqlInfo

	RedisManager GPGameSdkRedisManager

	userContentInfo []base.ServerAddr

	cacheTime   int
	cacheSwitch bool

	appIdThirdArr          []AppIdThirdMap
	specialAppIdMapArr     [][]uint64
	blockSecondAppidMapArr [][]uint64
	thirdProtectAppIdArr   []uint64
	thirdDeviceAppIdArr    []uint64
	filterUcidArr          []uint64
	insertLogFile          string
	thriftManager          GPThriftManager
	reportAddr             string

	//指定渠道号,生成的game_uin以LL开头  weijun.xie 20190610
	ChangLLFirstList []uint64
}

func (self *GPGameUinManager) SetChangLLFirstList(list []uint64) {
	self.ChangLLFirstList = list
	logger.Logln(logger.DEBUG, "change_ll_first_list is : ", self.ChangLLFirstList)
}

func (self *GPGameUinManager) SetServerInfo(addr string) {
	self.addr = addr
	data := strings.Split(addr, ":")
	if len(data) == 2 {
		self.reportAddr = utils.GetServerIP(true) + ":" + data[1]
	}
	self.thriftManager.SetReportAddr(self.reportAddr)
}

func (self *GPGameUinManager) SetConstUrls(urls ...string) {
	if len(urls) >= 3 {
		self.wdjUinUrl = urls[0]
		self.wdjUinNewUrl = urls[1]
		self.pptvUinUrl = urls[2]
	}
}

func (self *GPGameUinManager) SetConstKeys(keys ...string) {
	if len(keys) >= 3 {
		self.wdjUinKey = keys[0]
		self.wdjPlatformId = keys[1]
		self.pptvUinKey = keys[2]
	}
}

func (self *GPGameUinManager) SetCacheTime(cacheTime int) {
	self.cacheTime = cacheTime
}

func (self *GPGameUinManager) SetCacheSwitch(cacheSwitch bool) {
	self.cacheSwitch = cacheSwitch
}

func (self *GPGameUinManager) SetInsertLogFile(insertLogFile string) {
	self.insertLogFile = insertLogFile
}

func (self *GPGameUinManager) SetSpecailAppIdArr(specialAppIdMapArr [][]uint64) {
	self.specialAppIdMapArr = specialAppIdMapArr
}

func (self *GPGameUinManager) SetBlockSecondAppIdArr(blockSecondAppidMapArr [][]uint64) {
	self.blockSecondAppidMapArr = blockSecondAppidMapArr
}

func (self *GPGameUinManager) SetThirdProtectAppIdArr(thridProtectAppIdArr []uint64) {
	self.thirdProtectAppIdArr = thridProtectAppIdArr
}

func (self *GPGameUinManager) SetThirdDeviceAppIdArr(thirdDeviceAppIdArr []uint64) {
	self.thirdDeviceAppIdArr = thirdDeviceAppIdArr
}

func (self *GPGameUinManager) SetFilterUcidArr(filterUcidArr []uint64) {
	self.filterUcidArr = filterUcidArr
}

func (self *GPGameUinManager) SetMysqlInfo(gpuserMysqlReaderInfo base.MysqlInfo, gpuserMysqlWriterInfo base.MysqlInfo, gpdevMysqlReaderInfo base.MysqlInfo, gpdevMysqlWriterInfo base.MysqlInfo) {
	self.SetGPUserMysqlReaderInfo(gpuserMysqlReaderInfo)
	self.SetGPUserMysqlWriterInfo(gpuserMysqlWriterInfo)
	self.SetGPDevMysqlReaderInfo(gpdevMysqlReaderInfo)
	self.SetGPDevMysqlWriterInfo(gpdevMysqlWriterInfo)
}

func (self *GPGameUinManager) SetGPUserMysqlReaderInfo(mysqlInfo base.MysqlInfo) {
	self.gpuserMysqlReaderInfo = mysqlInfo
}

func (self *GPGameUinManager) SetGPUserMysqlWriterInfo(mysqlInfo base.MysqlInfo) {
	self.gpuserMysqlWriterInfo = mysqlInfo
}

func (self *GPGameUinManager) SetGPDevMysqlReaderInfo(mysqlInfo base.MysqlInfo) {
	self.gpdevMysqlReaderInfo = mysqlInfo
}

func (self *GPGameUinManager) SetGPDevMysqlWriterInfo(mysqlInfo base.MysqlInfo) {
	self.gpdevMysqlWriterInfo = mysqlInfo
}

func (self *GPGameUinManager) SetReceiveUrls(receiveUrls []string) {
	self.thriftManager.SetReceiveUrls(receiveUrls)
}

func (self *GPGameUinManager) SetReportUrls(reportUrls []string) {
	self.thriftManager.SetReportUrls(reportUrls)
}

func (self *GPGameUinManager) SetDirs(dirs []string) {
	self.thriftManager.SetDirs(dirs)
}

func (self *GPGameUinManager) SetOptIdcs(idcs []string) {
	self.thriftManager.SetOptIdcs(idcs)
}

func (self *GPGameUinManager) SetIDC(idc string) {
	self.thriftManager.SetIdc(idc)
}

func (self *GPGameUinManager) SetServerType(serverType string) {
	self.thriftManager.SetServerType(serverType)
}
func (self *GPGameUinManager) SetStage(stage int) {
	self.thriftManager.SetStage(stage)
}

func (self *GPGameUinManager) SetETCDKey(key string) {
	self.thriftManager.SetKey(key)
}

func (self *GPGameUinManager) Run() error {
	self.thriftManager.Init()
	self.worker = &GPGameUinWorker{}
	self.worker.SetMysqlInfo(self.gpuserMysqlReaderInfo, self.gpuserMysqlWriterInfo, self.gpdevMysqlReaderInfo, self.gpdevMysqlWriterInfo)
	self.worker.manager = self
	self.worker.Init()
	transportFactory := thrift.NewTBufferedTransportFactory(1024)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(self.addr)
	if err != nil {
		logger.Logln(logger.ERROR, err, self.addr)
		return err
	}
	processor := tbase.NewFlamingoBaseServiceProcessor(self)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	return server.Serve()
}

func (self *GPGameUinManager) Any(req *tbase.BinDataReq) (*tbase.BinDataRes, error) {
	var err error
	var binBody []byte
	res := &tbase.BinDataRes{
		Status: 0,
	}
	sspkg, errReq := self.getData(req.Req)
	if errReq == nil {
		Cmd := sspkg.GetClientHead().GetCmd()
		if Cmd != uint32(SXXGameUinProto_CMD_CMD_SXXGameUinProto) {
			res.Status = 1999
			return res, nil
		}
		SXXGameUinProtoResStr, errRes := self.HandleRequest(sspkg)
		if errRes == nil {
			sspkg.Body = []byte(SXXGameUinProtoResStr)
			binBody, err = proto.Marshal(sspkg)
			if err != nil {
				logger.Logln(logger.ERROR, err)
				res.Status = 1999
				return res, nil
			}
			res.Res = binBody
		} else {
			logger.Logln(logger.ERROR, errRes)
			res.Status = 1999
			return res, nil
		}
	} else {
		logger.Logln(logger.ERROR, errReq)
		res.Status = 1999
		return res, nil
	}
	return res, nil
}

func (self *GPGameUinManager) HandleRequest(sspkg *XXUnitySSPkg) (string, error) {
	var returnStr string
	var response *SXXGameUinProto
	SXXGameUinProtoReq, err := self.getBody(sspkg)
	if err != nil {
		return "", err
	}
	logger.Logln(logger.DEBUG, SXXGameUinProtoReq)
	subcmd := SXXGameUinProtoReq.GetSubcmd()
	userInfo := sspkg.GetClientHead().GetUserInfo()
	ip := sspkg.GetServerHead().GetClientIp()
	ipStr := utils.InetNtoa(ip)
	switch subcmd {
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINREQ):
		C.OssAttrInc(159, 0, 1)
		response = self.worker.handleGetAppGameUinReq(SXXGameUinProtoReq, userInfo, ipStr)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDFROMGAMEUINREQ):
		C.OssAttrInc(159, 1, 1)
		response = self.worker.handleGetUidFromGameUinReq(SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDALLAPPIDANDGAMEUINREQ):
		C.OssAttrInc(159, 2, 1)
		response = self.worker.handleGetUidAllAppidAndGameUinReq(SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUCIDALLAPPIDANDGAMEUINREQ):
		C.OssAttrInc(159, 3, 1)
		returnStr, _ = self.worker.handleGetUcidAllAppidAndGameUinReq(SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINWITHOUTCREATEREQ):
		C.OssAttrInc(159, 4, 1)
		response = self.worker.handleGetAppGameUinWithoutCreate(SXXGameUinProtoReq, userInfo, ipStr)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_UPDATEUCIDREQ):
		C.OssAttrInc(159, 5, 1)
		response = self.worker.handleUpdateUcidReq(SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_CREATESUBGAMEUINREQ):
		response = self.worker.handleCreateSubGameUinReq(sspkg, SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDANDAPPIDALLGAMEUINREQ):
		response = self.worker.handleGetUidAndAppidAllGameUinReq(sspkg, SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_CLEARGAMEUINORLOGINTIMECACHEREQ):
		response = self.worker.handleClearCacheReq(sspkg, SXXGameUinProtoReq)
	case int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_UPDATEGAMEUINREQ):
		response = self.worker.handleUpdateInfo(sspkg, SXXGameUinProtoReq)
	default:
		return "", errors.New("wrong subcmd")
	}
	logger.Logln(logger.ERROR, SXXGameUinProtoReq, response)
	if response != nil {
		returnStr, _ = self.setData(response)
	}
	return returnStr, nil
}

func (self *GPGameUinManager) setData(pkg *SXXGameUinProto) (string, error) {
	bytes, err := proto.Marshal(pkg)
	if err != nil {
		logger.Logln(logger.DEBUG, "SetData failed, errmsg:", err)
		return "", err
	}
	return string(bytes), nil
}

func (self *GPGameUinManager) getData(bytes []byte) (*XXUnitySSPkg, error) {
	sspkg := &XXUnitySSPkg{}
	err := proto.Unmarshal(bytes, sspkg)
	if err != nil {
		logger.Logln(logger.DEBUG, "Parse byte[] to XXUnitySSPkg failed, errmsg:", err)
		return nil, err
	}
	return sspkg, nil
}

func (self *GPGameUinManager) getBody(sspkg *XXUnitySSPkg) (*SXXGameUinProto, error) {
	body := sspkg.GetBody()
	subPkg := &SXXGameUinProto{}
	err := proto.Unmarshal(body, subPkg)
	if err != nil {
		return nil, err
	}
	return subPkg, nil
}
