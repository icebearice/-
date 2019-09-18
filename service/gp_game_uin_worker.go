package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"errors"
	"flamingo/base"
	"flamingo/logger"
	"flamingo/utils"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
)

type GPGameUinWorker struct {
	manager                     *GPGameUinManager
	gpSdkUserMysqlReaderHandler *GPSDKUserMysqlReaderHandler
	gpSdkUserMysqlWriterHandler *GPSDKUserMysqlWriterHandler
	gpDevMysqlReaderHandler     *GPDevMysqlReaderHandler
	gpDevMysqlWriterHandler     *GPDevMysqlWriterHandler
	utilsManager                UtilsManager
	helper                      GPGameUinHelper
}

func (self *GPGameUinWorker) Init() {
	self.utilsManager.worker = self
	self.utilsManager.manager = self.manager
	self.utilsManager.Init()
	self.helper.worker = self
	self.helper.Init()
	self.helper.utilsManager = self.utilsManager
}

func (self *GPGameUinWorker) SetMysqlInfo(gpuserMysqlReaderInfo base.MysqlInfo, gpuserMysqlWriterInfo base.MysqlInfo, gpdevMysqlReaderInfo base.MysqlInfo, gpdevMysqlWriterInfo base.MysqlInfo) {
	self.SetGPUserMysqlReaderInfo(gpuserMysqlReaderInfo)
	self.SetGPUserMysqlWriterInfo(gpuserMysqlWriterInfo)
	self.SetGPDevMysqlReaderInfo(gpdevMysqlReaderInfo)
	self.SetGPDevMysqlWriterInfo(gpdevMysqlWriterInfo)
}

func (self *GPGameUinWorker) SetGPUserMysqlReaderInfo(mysqlInfo base.MysqlInfo) {
	self.gpSdkUserMysqlReaderHandler = &GPSDKUserMysqlReaderHandler{}
	self.gpSdkUserMysqlReaderHandler.SetMysqlInfo(mysqlInfo)
	self.gpSdkUserMysqlReaderHandler.worker = self
	err := self.gpSdkUserMysqlReaderHandler.Init()
	if err != nil {
		logger.Logln(logger.ERROR, err)
	} else {
		logger.Logln(logger.DEBUG, "GPSDKUserMysqlReaderHandler connected")
	}
}

func (self *GPGameUinWorker) SetGPUserMysqlWriterInfo(mysqlInfo base.MysqlInfo) {
	self.gpSdkUserMysqlWriterHandler = &GPSDKUserMysqlWriterHandler{}
	self.gpSdkUserMysqlWriterHandler.SetMysqlInfo(mysqlInfo)
	self.gpSdkUserMysqlWriterHandler.worker = self
	err := self.gpSdkUserMysqlWriterHandler.Init()
	if err != nil {
		logger.Logln(logger.ERROR, err)
	} else {
		logger.Logln(logger.DEBUG, "GPSDKUserMysqlWriterHandler connected")
	}
}

func (self *GPGameUinWorker) SetGPDevMysqlReaderInfo(mysqlInfo base.MysqlInfo) {
	self.gpDevMysqlReaderHandler = &GPDevMysqlReaderHandler{}
	self.gpDevMysqlReaderHandler.SetMysqlInfo(mysqlInfo)
	self.gpDevMysqlReaderHandler.worker = self
	err := self.gpDevMysqlReaderHandler.Init()
	if err != nil {
		logger.Logln(logger.ERROR, err)
	} else {
		logger.Logln(logger.DEBUG, "GPDevMysqlReaderHandler connected")
	}
}

func (self *GPGameUinWorker) SetGPDevMysqlWriterInfo(mysqlInfo base.MysqlInfo) {
	self.gpDevMysqlWriterHandler = &GPDevMysqlWriterHandler{}
	self.gpDevMysqlWriterHandler.SetMysqlInfo(mysqlInfo)
	self.gpDevMysqlWriterHandler.worker = self
	err := self.gpDevMysqlWriterHandler.Init()
	if err != nil {
		logger.Logln(logger.ERROR, err)
	} else {
		logger.Logln(logger.DEBUG, "GPDevMysqlWriterHandler connected")
	}
}

func (self *GPGameUinWorker) handleGetAppGameUinReq(reqProto *SXXGameUinProto, userInfo *UserInfo, ip string) *SXXGameUinProto {
	C.OssAttrInc(110, 4, 1)
	C.OssAttrInc(159, 28, 1)
	response := &SXXGameUinProto{
		Result:           proto.Int32(0),
		Subcmd:           proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINRES)),
		GetAppGameUinRes: &GetAppGameUinRes{},
	}
	appid := reqProto.GetGetAppGameUinReq().GetAppid()
	uid := reqProto.GetGetAppGameUinReq().GetUid()
	cid := reqProto.GetGetAppGameUinReq().GetCid()
	ucid := self.utilsManager.getCidByUid(uid)
	uuid := userInfo.GetUuid()
	remark := self.helper.getDefaultRemark(uid)
	gameUinInfo := self.helper.getRecentLoginGameUinInfo(appid, uid)
	if gameUinInfo != nil {
		if len(gameUinInfo.Remark) <= 0 {
			gameUinInfo.Remark = remark
			setType := []uint32{uint32(SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Remark)}
			self.helper.updateGameUin(appid, uid, setType, gameUinInfo)
		}
		response.GetAppGameUinRes.Success = proto.Bool(true)
		response.GetAppGameUinRes.GameUin = proto.String(gameUinInfo.GameUin)
		response.GetAppGameUinRes.GameUinInfo = self.packageGameUinProto(gameUinInfo)
		return response
	}
	appidPPTV := self.utilsManager.getThirdPPTVAppId(appid)
	if len(appidPPTV) > 0 && self.helper.getPPTVGameUin(response, appid, uid, cid, ucid, appidPPTV, uuid, ip) {
		return response
	}

	C.OssAttrInc(110, 9, 1)
	if self.helper.shouldBlockGameUin(response, appid, uid, ucid) {
		C.OssAttrInc(159, 37, 1)
		logger.Logln(logger.ERROR, "have block")
		return response
	}

	if self.helper.getSpecialGameUin(response, appid, uid, ucid, cid, uuid, ip) {
		return response
	}
	if self.helper.getWDJGameUin(response, appid, uid, ucid, cid, uuid, ip) {
		C.OssAttrInc(159, 41, 1)
		return response
	}
	info := self.helper.createSubGameUin(appid, uid, ucid, cid, uuid, ip, remark, userInfo)
	if info != nil {
		C.OssAttrInc(159, 42, 1)
		response.GetAppGameUinRes.Success = proto.Bool(true)
		response.GetAppGameUinRes.GameUin = info.GameUin
		response.GetAppGameUinRes.GameUinInfo = info
		return response
	}
	response.GetAppGameUinRes.Success = proto.Bool(false)
	return response
}

func (self *GPGameUinWorker) handleGetUidFromGameUinReq(SXXGameUinProtoReq *SXXGameUinProto) *SXXGameUinProto {
	C.OssAttrInc(110, 5, 1)
	appid := SXXGameUinProtoReq.GetGetUidFromGameUinReq().GetAppid()
	gameUin := SXXGameUinProtoReq.GetGetUidFromGameUinReq().GetGameUin()
	response := &SXXGameUinProto{
		Result:               proto.Int32(0),
		Subcmd:               proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDFROMGAMEUINRES)),
		GetUidFromGameUinRes: &GetUidFromGameUinRes{},
	}
	uid := self.helper.getUserUidFromGameUin(gameUin, appid)
	response.GetUidFromGameUinRes.Uid = proto.Uint64(uid)
	if uid > 0 {
		response.GetUidFromGameUinRes.Success = proto.Bool(true)
	} else {
		response.GetUidFromGameUinRes.Success = proto.Bool(false)
	}
	return response
}

func (self *GPGameUinWorker) handleGetUidAllAppidAndGameUinReq(SXXGameUinProtoReq *SXXGameUinProto) *SXXGameUinProto {
	C.OssAttrInc(110, 6, 1)
	response := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDALLAPPIDANDGAMEUINRES)),
		GetUidAllAppidAndGameUinRes: &GetUidAllAppidAndGameUinRes{
			Success:            proto.Bool(true),
			AppidAndGameUinArr: []*AppidAndGameUin{},
		},
	}
	uid := SXXGameUinProtoReq.GetGetUidAllAppidAndGameUinReq().GetUid()
	infos := self.helper.getAllGameUin(uid)
	if len(infos) <= 0 {
		return response
	}
	for _, info := range infos {
		response.GetUidAllAppidAndGameUinRes.AppidAndGameUinArr = append(response.GetUidAllAppidAndGameUinRes.AppidAndGameUinArr, self.packageGameUinProto(info))
	}
	return response
}

func (self *GPGameUinWorker) handleGetUcidAllAppidAndGameUinReq(SXXGameUinProtoReq *SXXGameUinProto) (string, error) {
	C.OssAttrInc(110, 7, 1)
	var returnStr string
	var err error
	ucid := SXXGameUinProtoReq.GetGetUcidAllAppidAndGameUinReq().GetUcid()
	starttime := SXXGameUinProtoReq.GetGetUcidAllAppidAndGameUinReq().GetTS()
	endtime := SXXGameUinProtoReq.GetGetUcidAllAppidAndGameUinReq().GetTE()
	SXXGameUinProtoRes := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUCIDALLAPPIDANDGAMEUINRES)),
	}
	returnStr, _ = self.manager.setData(SXXGameUinProtoRes)
	getUcidAllAppidAndGameUinRes := &GetUcidAllAppidAndGameUinRes{}
	appids, errq := self.utilsManager.getDevAppIds()
	if errq != nil {
		logger.Logln(logger.ERROR, errq)
		return returnStr, errq
	}
	if len(appids) <= 0 {
		logger.Logln(logger.ERROR, "get dev appids fail")
		return returnStr, errors.New("get dev appids fail")
	}
	SAppidAndGameUinArr, errq := self.gpSdkUserMysqlReaderHandler.getUcidAllAppidAndGameUin(ucid, starttime, endtime, appids)
	if errq != nil {
		logger.Logln(logger.ERROR, errq)
		return returnStr, errq
	}
	AppidAndGameUinArr := self.utilsManager.SetAppIdAndGameUin(SAppidAndGameUinArr)
	logger.Logln(logger.DEBUG, SAppidAndGameUinArr)

	getUcidAllAppidAndGameUinRes.Success = proto.Bool(true)
	getUcidAllAppidAndGameUinRes.AppidAndGameUinArr = AppidAndGameUinArr
	SXXGameUinProtoRes.GetUcidAllAppidAndGameUinRes = getUcidAllAppidAndGameUinRes
	returnStr, err = self.manager.setData(SXXGameUinProtoRes)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return returnStr, err
	}
	return returnStr, nil
}

func (self *GPGameUinWorker) handleGetAppGameUinWithoutCreate(reqProto *SXXGameUinProto, uinfo *UserInfo, ip string) *SXXGameUinProto {
	C.OssAttrInc(110, 62, 1)
	logger.Logln(logger.ERROR, reqProto)
	response := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINWITHOUTCREATERES)),
		GetAppGameUinWithoutCreateRes: &GetAppGameUinWithoutCreateRes{},
		GetAppGameUinRes:              &GetAppGameUinRes{},
	}
	appid := reqProto.GetGetAppGameUinWithoutCreateReq().GetAppid()
	uid := reqProto.GetGetAppGameUinWithoutCreateReq().GetUid()
	ucid := self.utilsManager.getCidByUid(uid)
	uuid := uinfo.GetUuid()
	gameUinInfo := self.helper.getRecentLoginGameUinInfo(appid, uid)
	if gameUinInfo != nil {
		response.GetAppGameUinWithoutCreateRes.Success = proto.Bool(true)
		response.GetAppGameUinWithoutCreateRes.GameUin = proto.String(gameUinInfo.GameUin)
		response.GetAppGameUinWithoutCreateRes.GameUinInfo = self.packageGameUinProto(gameUinInfo)
		response.GetAppGameUinRes = nil
		return response
	}
	self.helper.getSpecialGameUin(response, appid, uid, ucid, 0, uuid, ip)
	response.GetAppGameUinWithoutCreateRes.Success = response.GetAppGameUinRes.Success
	response.GetAppGameUinWithoutCreateRes.GameUin = response.GetAppGameUinRes.GameUin
	response.GetAppGameUinWithoutCreateRes.GameUinInfo = response.GetAppGameUinRes.GameUinInfo
	response.GetAppGameUinRes = nil
	return response
}

func (self *GPGameUinWorker) handleUpdateUcidReq(SXXGameUinProtoReq *SXXGameUinProto) *SXXGameUinProto {
	C.OssAttrInc(110, 54, 1)
	response := &SXXGameUinProto{
		Result:        proto.Int32(0),
		Subcmd:        proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_UPDATEUCIDRES)),
		UpdateUcidRes: &UpdateUcidRes{},
	}
	appid := SXXGameUinProtoReq.GetUpdateUcidReq().GetAppid()
	uid := SXXGameUinProtoReq.GetUpdateUcidReq().GetUid()
	newUcid := SXXGameUinProtoReq.GetUpdateUcidReq().GetNewUcid()
	gameUinInfo := self.helper.getRecentLoginGameUinInfo(appid, uid)
	if gameUinInfo == nil || len(gameUinInfo.GameUin) <= 0 {
		response.UpdateUcidRes.Success = proto.Bool(false)
		response.UpdateUcidRes.ErrMsg = proto.String("appid,uid not find gameUin")
		return response
	}
	setType := []uint32{uint32(SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Ucid)}
	info := &SAppidAndGameUin{
		Uid:     uid,
		Appid:   appid,
		GameUin: gameUinInfo.GameUin,
		Ucid:    newUcid,
	}
	if !self.helper.updateGameUin(appid, uid, setType, info) {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_DB))
		return response
	}
	return response
}

func (self *GPGameUinWorker) handleCreateSubGameUinReq(request *XXUnitySSPkg, reqProto *SXXGameUinProto) *SXXGameUinProto {
	response := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_CREATESUBGAMEUINRES)),
		CreateSubGameUinRes: &CreateSubGameUinRes{
			Info: &AppidAndGameUin{},
		},
	}
	if reqProto == nil || reqProto.CreateSubGameUinReq == nil || reqProto.CreateSubGameUinReq.Appid == nil || reqProto.CreateSubGameUinReq.Uid == nil || reqProto.CreateSubGameUinReq.Remark == nil {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_None))
		return response
	}
	if request == nil || request.ServerHead == nil || request.ClientHead == nil || request.ServerHead.ClientIp == nil || request.ClientHead.UserInfo == nil {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	appid := reqProto.CreateSubGameUinReq.GetAppid()
	uid := reqProto.CreateSubGameUinReq.GetUid()
	remark := reqProto.CreateSubGameUinReq.GetRemark()
	if appid <= 0 || uid <= 0 || len(remark) <= 0 {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	ip := utils.InetNtoa(request.ServerHead.GetClientIp())
	uuid := request.ClientHead.UserInfo.GetUuid()
	ucid := self.utilsManager.getCidByUid(uid)
	userInfo := request.ClientHead.GetUserInfo()
	cid := uint64(userInfo.GetChannelID())
	info := self.helper.createSubGameUin(appid, uid, ucid, cid, uuid, ip, remark, userInfo)
	if info != nil {
		response.CreateSubGameUinRes.Info = info
		return response
	}
	response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
	return response
}

func (self *GPGameUinWorker) handleGetUidAndAppidAllGameUinReq(request *XXUnitySSPkg, reqProto *SXXGameUinProto) *SXXGameUinProto {
	var response *SXXGameUinProto
	response = &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETUIDANDAPPIDALLGAMEUINRES)),
		GetUidAndAppidAllGameUinRes: &GetUidAndAppidAllGameUinRes{
			Infos: []*AppidAndGameUin{},
		},
	}
	if reqProto == nil || reqProto.GetUidAndAppidAllGameUinReq == nil || reqProto.GetUidAndAppidAllGameUinReq.Uid == nil || reqProto.GetUidAndAppidAllGameUinReq.Appid == nil {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_None))
		return response
	}
	uid := reqProto.GetUidAndAppidAllGameUinReq.GetUid()
	appid := reqProto.GetUidAndAppidAllGameUinReq.GetAppid()
	if appid <= 0 || uid <= 0 {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	infos := self.helper.getGameUin(appid, uid)
	if infos == nil || len(infos) <= 0 {
		return response
	}
	for _, info := range infos {
		if len(info.Remark) <= 0 {
			info.Remark = self.helper.getDefaultRemark(uid)
			setType := []uint32{uint32(SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Remark)}
			self.helper.updateGameUin(appid, uid, setType, info)
		}
		protoInfo := self.packageGameUinProto(info)
		response.GetUidAndAppidAllGameUinRes.Infos = append(response.GetUidAndAppidAllGameUinRes.Infos, protoInfo)
	}
	return response
}

func (self *GPGameUinWorker) handleClearCacheReq(request *XXUnitySSPkg, reqProto *SXXGameUinProto) *SXXGameUinProto {
	response := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_CLEARGAMEUINORLOGINTIMECACHERES)),
	}
	logger.Logln(logger.DEBUG, reqProto)
	uid := reqProto.ClearGameUinOrLoginTimeCacheReq.GetUid()
	appid := reqProto.ClearGameUinOrLoginTimeCacheReq.GetAppid()
	tType := reqProto.ClearGameUinOrLoginTimeCacheReq.GetType()
	if uid <= 0 || appid <= 0 || tType <= 0 {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	var res bool
	if tType == 1 {
		res = self.manager.RedisManager.delCacheInfo(appid, uid)
		self.manager.RedisManager.delAllGameUin(uid, self.utilsManager.getAppidIdx(appid))
	} else if tType == 2 {
		res = self.manager.RedisManager.delLoginGameUin(appid, uid)
	}
	if !res {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
	}
	return response
}

func (self *GPGameUinWorker) handleUpdateInfo(request *XXUnitySSPkg, reqProto *SXXGameUinProto) *SXXGameUinProto {
	response := &SXXGameUinProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_UPDATEGAMEUINRES)),
	}

	if reqProto == nil || reqProto.UpdateGameUinReq == nil || reqProto.UpdateGameUinReq.SetType == nil || reqProto.UpdateGameUinReq.Info == nil {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	setType := reqProto.UpdateGameUinReq.GetSetType()
	info := reqProto.UpdateGameUinReq.GetInfo()
	if len(setType) <= 0 || info.Appid == nil || info.Uid == nil || info.GameUin == nil {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_Unknown))
		return response
	}
	appid := info.GetAppid()
	uid := info.GetUid()
	if !self.helper.updateGameUin(appid, uid, setType, self.unPackageGameUinProto(info)) {
		response.Result = proto.Int32(int32(SXXGameUinProtoErrorCode_SXXGameUin_Err_DB))
		return response
	}
	return response
}

func (self *GPGameUinWorker) packageLoginLogProto(info *GameUinLogin) *SXXGameUinLoginLog {
	protoInfo := &SXXGameUinLoginLog{
		Uid:       proto.Uint64(info.Uid),
		Appid:     proto.Uint64(info.Appid),
		GameUin:   proto.String(info.GameUin),
		LoginTime: proto.Uint64(info.LoginTime),
	}
	return protoInfo
}

func (self *GPGameUinWorker) packageGameUinProto(info *SAppidAndGameUin) *AppidAndGameUin {
	protoInfo := &AppidAndGameUin{
		Uid:            proto.Uint64(info.Uid),
		Appid:          proto.Uint64(info.Appid),
		GameUin:        proto.String(info.GameUin),
		Addtime:        proto.Uint64(info.Addtime),
		Cid:            proto.Uint64(info.Cid),
		Ucid:           proto.Uint64(info.Ucid),
		Remark:         proto.String(info.Remark),
		RechargeAmount: proto.Uint64(info.RechargeAmount),
	}
	return protoInfo
}

func (self *GPGameUinWorker) unPackageGameUinProto(protoInfo *AppidAndGameUin) *SAppidAndGameUin {
	info := &SAppidAndGameUin{
		Uid:            protoInfo.GetUid(),
		Appid:          protoInfo.GetAppid(),
		GameUin:        protoInfo.GetGameUin(),
		Addtime:        protoInfo.GetAddtime(),
		Cid:            protoInfo.GetCid(),
		Ucid:           protoInfo.GetUcid(),
		Remark:         protoInfo.GetRemark(),
		RechargeAmount: protoInfo.GetRechargeAmount(),
	}
	return info
}
