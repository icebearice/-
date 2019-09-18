package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"encoding/json"
	"flamingo/logger"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	"sort"
	"time"
)

type GPGameUinHelper struct {
	worker                *GPGameUinWorker
	utilsManager          UtilsManager
	cacheTime             int64
	thriftManager         *GPThriftManager
	uinToGameUinCacheSize int
	gameUinToUinCacheSize int
}

func (self *GPGameUinHelper) Init() {
	self.thriftManager = &self.worker.manager.thriftManager
	self.uinToGameUinCacheSize = 100
	self.gameUinToUinCacheSize = 100
	if self.cacheTime <= 60 {
		self.cacheTime = 60 * 60 * 2
	}
}

func (self *GPGameUinHelper) getUserUidFromGameUin(gameUin string, appid uint64) uint64 {
	uid, err := self.worker.gpSdkUserMysqlReaderHandler.getUidFromGameUin(appid, gameUin)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0
	}
	return uid
}

func (self *GPGameUinHelper) getSubPPTVGameUin(response *SXXGameUinProto, appid, uid, cid, ucid uint64, appidPPTV, uuid, ip string) bool {
	pptvRes := self.worker.utilsManager.generatePPTVUin(appidPPTV, uid)
	if len(pptvRes.Data.UserId) <= 0 {
		logger.Logln(logger.ERROR, "getPPTVGameUinFail")
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return true
	}
	infos := self.worker.gpSdkUserMysqlReaderHandler.getSubGameUin(appid, uid)
	ext, _ := json.Marshal(pptvRes)
	if len(infos) <= 0 {
		gameUin := pptvRes.Data.UserId
		remark := self.getDefaultRemark(uid)
		insertRes, err := self.worker.gpSdkUserMysqlWriterHandler.insertSubGameUin(uid, appid, gameUin, ucid, cid, uuid, ip, remark)
		if insertRes != true || err != nil {
			logger.Logln(logger.ERROR, err)
			logger.Logln(logger.ERROR, "insert gameuin fail")
		}
		protoInfo := &AppidAndGameUin{
			Uid:            proto.Uint64(uint64(uid)),
			Appid:          proto.Uint64(uint64(appid)),
			GameUin:        proto.String(gameUin),
			Addtime:        proto.Uint64(uint64(time.Now().Unix())),
			Cid:            proto.Uint64(uint64(cid)),
			Remark:         proto.String(remark),
			Ext:            proto.String(string(ext)),
			RechargeAmount: proto.Uint64(uint64(0)),
		}
		if ucid > 0 {
			protoInfo.Ucid = proto.Uint64(ucid)
		}
		response.GetUidAndAppidAllGameUinRes.Infos = append(response.GetUidAndAppidAllGameUinRes.Infos, protoInfo)
	} else {
		for _, info := range infos {
			protoInfo := &AppidAndGameUin{
				Uid:            proto.Uint64(uint64(info.Uid)),
				Appid:          proto.Uint64(uint64(info.Appid)),
				GameUin:        proto.String(info.GameUin),
				Addtime:        proto.Uint64(uint64(info.Addtime)),
				Cid:            proto.Uint64(uint64(info.Cid)),
				Remark:         proto.String(info.Remark),
				Ext:            proto.String(string(ext)),
				RechargeAmount: proto.Uint64(uint64(info.RechargeAmount)),
			}
			if info.Ucid > 0 {
				protoInfo.Ucid = proto.Uint64(ucid)
			}
			response.GetUidAndAppidAllGameUinRes.Infos = append(response.GetUidAndAppidAllGameUinRes.Infos, protoInfo)
		}
	}
	return true
}

func (self *GPGameUinHelper) getPPTVGameUin(response *SXXGameUinProto, appid, uid, cid, ucid uint64, appidPPTV, uuid, ip string) bool {
	C.OssAttrInc(159, 33, 1)
	pptvRes := self.worker.utilsManager.generatePPTVUin(appidPPTV, uid)
	if len(pptvRes.Data.UserId) <= 0 {
		response.GetAppGameUinRes.Success = proto.Bool(false)
		response.GetAppGameUinRes.GameUin = proto.String("getPPTVGameUinFail")
		C.OssAttrInc(159, 34, 1)
		return true
	}
	gameUins := self.getGameUin(appid, uid)
	if len(gameUins) <= 0 {
		gameUin := pptvRes.Data.UserId
		insertRes, erri := self.worker.gpSdkUserMysqlWriterHandler.insertGameUin(uid, appid, gameUin, ucid, cid, uuid, ip)
		if insertRes != true || erri != nil {
			logger.Logln(logger.ERROR, erri)
			logger.Logln(logger.ERROR, "insert gameuin fail")
		}
	}
	response.GetAppGameUinRes.Success = proto.Bool(true)
	response.GetAppGameUinRes.GameUin = proto.String(pptvRes.Data.UserId)
	response.GetAppGameUinRes.GameUinInfo = &AppidAndGameUin{}
	response.GetAppGameUinRes.GameUinInfo.Uid = proto.Uint64(uid)
	response.GetAppGameUinRes.GameUinInfo.Appid = proto.Uint64(appid)
	response.GetAppGameUinRes.GameUinInfo.Addtime = proto.Uint64(uint64(time.Now().Unix()))
	response.GetAppGameUinRes.GameUinInfo.GameUin = proto.String(pptvRes.Data.UserId)
	response.GetAppGameUinRes.GameUinInfo.Cid = proto.Uint64(cid)
	response.GetAppGameUinRes.GameUinInfo.RechargeAmount = proto.Uint64(uint64(0))
	remark := self.getDefaultRemark(uid)
	response.GetAppGameUinRes.GameUinInfo.Remark = proto.String(remark)
	if ucid > 0 {
		response.GetAppGameUinRes.GameUinInfo.Ucid = proto.Uint64(ucid)
	}
	ext, _ := json.Marshal(pptvRes)
	response.GetAppGameUinRes.GameUinInfo.Ext = proto.String(string(ext))
	return true
}

func (self *GPGameUinHelper) getGameUin(appid uint64, uid uint64) []*SAppidAndGameUin {
	var data []*SAppidAndGameUin
	if self.worker.manager.cacheSwitch {
		data = self.worker.manager.RedisManager.getCacheInfo(appid, uid)
	}
	if data == nil || len(data) <= 0 {
		data = self.worker.gpSdkUserMysqlReaderHandler.getSubGameUin(appid, uid)
	}
	if data == nil || len(data) <= 0 {
		return nil
	}
	if self.worker.manager.cacheSwitch {
		self.worker.manager.RedisManager.setCacheInfo(appid, uid, data)
	}
	logins := self.getGameUinLoginTime(appid, uid)
	for k, v := range data {
		if login, ok := logins[v.GameUin]; ok {
			data[k].LoginTime = login.LoginTime
		} else {
			data[k].LoginTime = 0
		}
	}
	sort.Sort(SortGameUin{data})
	return data
}

func (self *GPGameUinHelper) getRecentLoginGameUinInfo(appid uint64, uid uint64) *SAppidAndGameUin {
	infos := self.getGameUin(appid, uid)
	if infos == nil || len(infos) <= 0 {
		return nil
	}
	return infos[0]
}

func (self *GPGameUinHelper) getGameUinLoginTime(appid uint64, uid uint64) map[string]*GameUinLogin {
	var gameUinLogin []*GameUinLogin
	gameUinLogin = self.worker.manager.RedisManager.getLoginGameUinByDesc(appid, uid)
	if len(gameUinLogin) <= 0 {
		gameUinLogin = self.worker.gpSdkUserMysqlReaderHandler.getLoginGameUinByDesc(appid, uid)
	}
	if len(gameUinLogin) <= 0 {
		logger.Logln(logger.ERROR, "get login_time error")
		return nil
	}
	self.worker.manager.RedisManager.setLoginGameUinByDesc(appid, uid, gameUinLogin)
	res := make(map[string]*GameUinLogin)
	for _, info := range gameUinLogin {
		res[info.GameUin] = info
	}
	return res
}

func (self *GPGameUinHelper) shouldBlockSubGameUin(response *SXXGameUinProto, appid, uid, ucid uint64) bool {
	appidFilterList := self.worker.utilsManager.getBlockAppIds()
	for _, filterAppid := range appidFilterList {
		if filterAppid == appid {
			C.OssAttrInc(110, 59, 1)
			return true
		}
	}
	if self.utilsManager.checkBlockCreate(appid, ucid) {
		return true
	}
	hardCodeAppidFilterList := []uint64{109570}
	for _, filterAppid := range hardCodeAppidFilterList {
		if filterAppid == appid {
			for _, v := range self.worker.manager.filterUcidArr {
				if ucid == v {
					return true
				}
			}
		}
	}
	firstAppid := self.utilsManager.getSecondBlockAppidMap(appid)
	if firstAppid <= 0 {
		return false
	}
	infos := self.getGameUin(appid, uid)
	if infos != nil && len(infos) > 0 {
		logger.Logln(logger.DEBUG, "该用户在已在第一个appid注册过了", firstAppid, appid)
		return true
	}
	logger.Logln(logger.DEBUG, "该用户并没有在第一个appid注册过了", firstAppid, appid)
	return false
}

func (self *GPGameUinHelper) shouldBlockGameUin(response *SXXGameUinProto, appid, uid, ucid uint64) bool {
	response.GetAppGameUinRes.Success = proto.Bool(false)
	response.GetAppGameUinRes.GameUin = proto.String("shouldBlockGameUin")
	appidFilterList := self.worker.utilsManager.getBlockAppIds()
	for _, filterAppid := range appidFilterList {
		if filterAppid == appid {
			C.OssAttrInc(110, 59, 1)
			return true
		}
	}
	isBlockCreate := self.utilsManager.checkBlockCreate(appid, ucid)
	if isBlockCreate {
		return true
	}
	hardCodeAppidFilterList := []uint64{109570}
	for _, filterAppid := range hardCodeAppidFilterList {
		if filterAppid == appid {
			for _, v := range self.worker.manager.filterUcidArr {
				if ucid == v {
					return true
				}
			}
		}
	}
	firstAppid := self.utilsManager.getSecondBlockAppidMap(appid)
	if firstAppid <= 0 {
		return false
	}
	gameUinInfo := self.getRecentLoginGameUinInfo(firstAppid, uid)
	if gameUinInfo != nil && len(gameUinInfo.GameUin) > 0 {
		response.GetAppGameUinRes.Success = proto.Bool(true)
		response.GetAppGameUinRes.GameUin = proto.String(gameUinInfo.GameUin)
		response.GetAppGameUinRes.GameUinInfo = self.worker.packageGameUinProto(gameUinInfo)
		logger.Logln(logger.DEBUG, "该用户在已在第一个appid注册过了", firstAppid, appid)
		return true
	}
	logger.Logln(logger.DEBUG, "该用户并没有在第一个appid注册过了", firstAppid, appid)
	return false
}

func (self *GPGameUinHelper) getSpecialSubGameUin(response *SXXGameUinProto, appid, uid, ucid, cid uint64, uuid, ip string) bool {
	appidMap := self.worker.utilsManager.getSpecialAppidMap(appid)
	if appidMap <= 0 {
		logger.Logln(logger.ERROR, "get special GameUin Failed")
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return false
	}
	C.OssAttrInc(110, 12, 1)
	infos := self.getGameUin(appidMap, uid)
	if infos == nil || len(infos) <= 0 {
		logger.Logln(logger.ERROR, "get special GameUin Failed")
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return false
	}
	var protoInfos []*AppidAndGameUin
	for _, info := range infos {
		if info.Remark == "" {
			info.Remark = self.getDefaultRemark(uid)
		}
		insertRes, err := self.worker.gpSdkUserMysqlWriterHandler.insertSubGameUin(uid, appid, info.GameUin, cid, ucid, uuid, ip, info.Remark)
		if insertRes != true || err != nil {
			logger.Logln(logger.ERROR, err)
			logger.Logln(logger.ERROR, "insert gameuin fail")
			return true
		}
		info.Uid = uid
		info.Appid = appid
		info.Cid = cid
		info.Ucid = ucid
		info.Addtime = uint64(time.Now().Unix())
		info.RechargeAmount = uint64(0)
		protoInfo := self.worker.packageGameUinProto(info)
		protoInfos = append(protoInfos, protoInfo)
	}
	response.GetUidAndAppidAllGameUinRes.Infos = protoInfos
	self.worker.manager.RedisManager.setCacheInfo(appid, uid, infos)
	C.OssAttrInc(159, 38, 1)
	return true
}

func (self *GPGameUinHelper) getSpecialGameUin(response *SXXGameUinProto, appid, uid, ucid, cid uint64, uuid, ip string) bool {
	appidMap := self.worker.utilsManager.getSpecialAppidMap(appid)
	if appidMap <= 0 {
		response.GetAppGameUinRes.Success = proto.Bool(false)
		response.GetAppGameUinRes.GameUin = proto.String("getspecialGameUinFailed")
		return false
	}
	C.OssAttrInc(110, 12, 1)
	info := self.getRecentLoginGameUinInfo(appidMap, uid)
	if info == nil || len(info.GameUin) <= 0 {
		response.GetAppGameUinRes.Success = proto.Bool(false)
		response.GetAppGameUinRes.GameUin = proto.String("getspecialGameUinFailed")
		return false
	}

	insertRes, erri := self.worker.gpSdkUserMysqlWriterHandler.insertGameUin(uid, appid, info.GameUin, cid, ucid, uuid, ip)
	if insertRes != true || erri != nil {
		logger.Logln(logger.ERROR, erri)
		logger.Logln(logger.ERROR, "insert gameuin fail")
		return true
	}
	info.Appid = appidMap
	response.GetAppGameUinRes.Success = proto.Bool(true)
	response.GetAppGameUinRes.GameUin = proto.String(info.GameUin)
	response.GetAppGameUinRes.GameUinInfo = self.worker.packageGameUinProto(info)
	C.OssAttrInc(159, 38, 1)
	return true
}

func (self *GPGameUinHelper) getWDJSubGameUin(response *SXXGameUinProto, appid, uid, ucid, cid uint64, uuid, ip string) bool {
	C.OssAttrInc(159, 39, 1)
	appidWdj := self.utilsManager.getThirdWdjAppId(appid)
	if len(appidWdj) <= 0 {
		return false
	}
	C.OssAttrInc(110, 10, 1)
	gameUin, err := self.utilsManager.generateWdjUinV2(appidWdj, uid)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return true
	}
	if len(gameUin) <= 0 {
		C.OssAttrInc(159, 40, 1)
		return false
	}
	remark := self.getDefaultRemark(uid)
	insertRes, err := self.worker.gpSdkUserMysqlWriterHandler.insertSubGameUin(uid, appid, gameUin, cid, ucid, uuid, ip, remark)
	if insertRes != true || err != nil {
		logger.Logln(logger.ERROR, err)
		logger.Logln(logger.ERROR, "insert gameuin fail")
		return true
	}
	now := time.Now().Unix()
	var infos []*SAppidAndGameUin
	info := &SAppidAndGameUin{}
	info.Uid = uid
	info.Appid = appid
	info.Addtime = uint64(now)
	info.Cid = cid
	info.GameUin = gameUin
	if ucid > 0 {
		info.Ucid = ucid
	}
	info.Remark = remark
	info.RechargeAmount = uint64(0)
	protoInfo := self.worker.packageGameUinProto(info)
	response.GetUidAndAppidAllGameUinRes.Infos = append(response.GetUidAndAppidAllGameUinRes.Infos, protoInfo)
	infos = append(infos, info)
	self.worker.manager.RedisManager.setCacheInfo(appid, uid, infos)
	return true
}

func (self *GPGameUinHelper) getWDJGameUin(response *SXXGameUinProto, appid, uid, ucid, cid uint64, uuid, ip string) bool {
	C.OssAttrInc(159, 39, 1)
	appidWdj := self.utilsManager.getThirdWdjAppId(appid)
	if len(appidWdj) <= 0 {
		return false
	}
	C.OssAttrInc(110, 10, 1)
	gameUin, err := self.utilsManager.generateWdjUinV2(appidWdj, uid)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return true
	}
	if len(gameUin) <= 0 {
		C.OssAttrInc(159, 40, 1)
		return false
	}
	remark := self.getDefaultRemark(uid)
	insertRes, erri := self.worker.gpSdkUserMysqlWriterHandler.insertGameUin(uid, appid, gameUin, cid, ucid, uuid, ip)
	if insertRes != true || erri != nil {
		logger.Logln(logger.ERROR, erri)
		logger.Logln(logger.ERROR, "insert gameuin fail")
		return true
	}
	response.GetAppGameUinRes.Success = proto.Bool(true)
	response.GetAppGameUinRes.GameUin = proto.String(gameUin)
	now := time.Now().Unix()
	info := &SAppidAndGameUin{}
	info.Uid = uid
	info.Appid = appid
	info.Addtime = uint64(now)
	info.Cid = cid
	info.GameUin = gameUin
	info.Remark = remark
	if ucid > 0 {
		info.Ucid = ucid
	}
	response.GetAppGameUinRes.GameUinInfo = self.worker.packageGameUinProto(info)
	var infos []*SAppidAndGameUin
	infos = append(infos, info)
	self.worker.manager.RedisManager.setCacheInfo(appid, uid, infos)
	return true
}

func (self *GPGameUinHelper) createSubGameUin(appid, uid, ucid, cid uint64, uuid, ip, remark string, uinfo *UserInfo) *AppidAndGameUin {
	gameUin := self.utilsManager.generateGameUin(appid, uid, ucid)
	if len(gameUin) <= 0 {
		return nil
	}
	gameUins := self.getGameUin(appid, uid)
	if gameUins != nil && len(gameUins) > 0 {
		for _, v := range gameUins {
			if remark == v.Remark {
				logger.Logln(logger.DEBUG, "remark repeat")
				return nil
			}
		}
	}
	insertRes, err := self.worker.gpSdkUserMysqlWriterHandler.insertSubGameUin(uid, appid, gameUin, cid, ucid, uuid, ip, remark)
	if insertRes != true || err != nil {
		return nil
	}
	for _, tpId := range self.worker.manager.thirdDeviceAppIdArr {
		if appid == tpId {
			appInfo := self.worker.gpDevMysqlReaderHandler.getAppidInfo(appid)
			pid := THIRDPART_ANDROID_PLATFORM
			if appInfo.PlatformId == uint64(PlatformType_PT_iOS) {
				pid = THIRDPART_IOS_PLATFORM
			}
			thirdGameUser := self.worker.utilsManager.getThirdGameUser(appid, pid, uid, gameUin, uuid, ip)
			lastinertID, _ := self.worker.gpDevMysqlWriterHandler.insertThirdGameUser(thirdGameUser)
			thirdDeviceInfo := ThirdDeviceInfo{
				Appid:    appid,
				DeviceID: uuid,
				Zuid:     uint64(lastinertID),
				Pid:      pid,
			}
			self.worker.gpDevMysqlWriterHandler.insertThirdDevice(thirdDeviceInfo)
		}
	}
	self.worker.manager.thriftManager.reportUserLog(uid, uinfo, appid, gameUin, ip)
	self.worker.manager.RedisManager.delCacheInfo(appid, uid)
	self.worker.manager.RedisManager.delAllGameUin(uid, self.worker.utilsManager.getAppidIdx(appid))
	return &AppidAndGameUin{
		Uid:     proto.Uint64(uid),
		Cid:     proto.Uint64(cid),
		Addtime: proto.Uint64(uint64(time.Now().Unix())),
		Appid:   proto.Uint64(appid),
		GameUin: proto.String(gameUin),
		Remark:  proto.String(remark),
		Ucid:    proto.Uint64(ucid),
	}
}

func (self *GPGameUinHelper) getDefaultRemark(uid uint64) string {
	remark := "未命名"
	userInfo, err := self.thriftManager.getUserInfo(uid)
	if err == nil && len(userInfo.GetBase().GetUnickname()) > 0 {
		remark = userInfo.GetBase().GetUnickname()
	}
	return remark
}

func (p SAppidAndGameUins) Len() int {
	return len(p)
}

func (p SAppidAndGameUins) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p SortGameUin) Less(i, j int) bool {
	if p.SAppidAndGameUins[i].LoginTime == p.SAppidAndGameUins[j].LoginTime {
		return p.SAppidAndGameUins[i].Addtime > p.SAppidAndGameUins[j].Addtime
	}
	return p.SAppidAndGameUins[i].LoginTime > p.SAppidAndGameUins[j].LoginTime
}

func (self *GPGameUinHelper) getAllGameUinByIndex(uid, index uint64) []*SAppidAndGameUin {
	data, err := self.worker.manager.RedisManager.getAllGameUin(uid, index)
	if err == nil {
		return data
	}
	data, err = self.worker.gpSdkUserMysqlReaderHandler.getGameUinsByUid(uid, index)
	if err == nil {
		self.worker.manager.RedisManager.setAllGameUin(uid, index, data)
	}
	return data
}

func (self *GPGameUinHelper) getAllGameUin(uid uint64) []*SAppidAndGameUin {
	maxAppid, _ := self.worker.gpDevMysqlReaderHandler.getMaxappid()
	maxIdx := self.worker.utilsManager.getAppidIdx(maxAppid)
	var gameUins, result []*SAppidAndGameUin
	gameUinsChan := make(chan []*SAppidAndGameUin, maxIdx)
	for idx := 0; idx < int(maxIdx); idx++ {
		go func() {
			res := self.getAllGameUinByIndex(uid, uint64(idx))
			gameUinsChan <- res
		}()
	}
	var i uint64
Loop:
	for {
		select {
		case gameUins = <-gameUinsChan:
			result = append(result, gameUins...)
			i += 1
			if i >= maxIdx {
				break Loop
			}
		case <-time.After(2 * time.Second):
			close(gameUinsChan)
			break Loop
		}
	}
	return result
}

func (self *GPGameUinHelper) updateGameUin(appid, uid uint64, setType []uint32, info *SAppidAndGameUin) bool {
	if !self.worker.gpSdkUserMysqlWriterHandler.updateGameUin(self.worker.utilsManager.getAppidIdx(appid), setType, info) {
		return false
	}
	for i := 0; i < 3; i++ {
		res := self.worker.manager.RedisManager.delCacheInfo(appid, uid)
		self.worker.manager.RedisManager.delAllGameUin(uid, self.worker.utilsManager.getAppidIdx(appid))
		if res {
			break
		}
	}
	return true
}
