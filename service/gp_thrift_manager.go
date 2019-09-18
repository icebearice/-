package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
//import "C"

import (
	"errors"
	"flamingo/flamingo_etcd"
	"flamingo/logger"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	"go/gen-go/GPUser"
	"time"
)

type GPThriftManager struct {
	flamingo_etcd.FlamingoThriftBaseManager
}

func (self *GPThriftManager) Init() {
	self.FlamingoThriftBaseManager.Init()
}

func (self *GPThriftManager) getUserInfo(uin uint64) (*GPUser.GpUser, error) {
	request := &GPUser.GetUserReq{
		UID: int32(uin),
	}
	server := self.GetETCDClient().GetGPUserServer("gp_user_content", flamingo_etcd.IdcsServerOpt(self.GetOptIDCS()), flamingo_etcd.StageServerOpt(self.GetStage()))
	if server == nil {
		logger.Logln(logger.ERROR, "not found any server")
		////C.OssAttrInc(150, 18, 1)
		return nil, errors.New("not found any server")
	}
	response, err := server.GetUser(request)
	if err != nil {
		//C.OssAttrInc(150, 18, 1)
		return nil, err
	}
	if response.Success != true {
		//C.OssAttrInc(150, 18, 1)
		return nil, err
	}
	return response.User, nil
}

func (self *GPThriftManager) reportUserLog(uin uint64, uinfo *UserInfo, appid uint64, gameUin, ip string) bool {
	reqProto := &SXXUserEventProto{
		Result: proto.Int32(0),
		Subcmd: proto.Int32(int32(SXXUserEventProto_SUBCMD_SUBCMD_SXXUserEventProto_SYNCSERVEREVENTREQ)),
		SyncServerInfoReq: &SXXUserEventSyncServerInfoReq{
			Infos: []*SXXUserEventSyncServerInfo{},
		},
	}
	reqProto.SyncServerInfoReq.Infos = append(reqProto.SyncServerInfoReq.Infos, &SXXUserEventSyncServerInfo{
		Uin:       proto.Uint64(uin),
		EventType: XXUserEventType_XXUserEventType_Create_Game_Uin.Enum(),
		Cmd:       proto.Uint32(uint32(SXXGameUinProto_CMD_CMD_SXXGameUinProto)),
		Subcmd:    proto.Uint32(uint32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINREQ)),
		Times:     proto.Uint64(uint64(time.Now().Unix())),
		Uinfo:     uinfo,
		Appid:     proto.Uint32(uint32(appid)),
		Ip:        proto.String(ip),
		GameUin:   proto.String(gameUin),
	})
	body, err := proto.Marshal(reqProto)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return false
	}
	req := &XXUnitySSPkg{
		ClientHead: &XXUnityCSPkgHead{
			Cmd:      proto.Uint32(uint32(SXXUserEventProto_CMD_CMD_SXXUserEventProto)),
			UserInfo: uinfo,
		},
		ServerHead: &XXUnitySSPkgHead{
			InterfaceIp:   proto.Uint32(0),
			InterfacePort: proto.Uint32(0),
			ClientIp:      proto.Uint32(0),
			ClientPort:    proto.Uint32(0),
			Flow:          proto.Uint64(0),
			PlatformType:  PlatformType_PT_None.Enum(),
		},
		Body: body,
	}
	response := self.SendSSRequest(req, "gp_user_log")
	if response == nil {
		logger.Logln(logger.ERROR, "response is nil")
		return false
	}
	return true
}
