package main

import (
	"encoding/json"
	"flag"
	"flamingo/base"
	"flamingo/thriftclient"
	"fmt"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	tbase "go/gen-go/base"
	"strconv"
	"strings"
	"time"
)

func main() {
	host := flag.String("addr", "192.168.139.128", "server host")
	port := flag.Int("port", 20201, "server port")
	appid := flag.Int("appid", 101101, "appid")
	//uid := flag.Int("uid", 2838848, "uid")
	uid := flag.Int("uid", 888888, "uid")
	cid := flag.Int("cid", 456, "channel id")
	ucid := flag.Int("ucid", 890, "user channer id")
	new_ucid := flag.Int("new_ucid", 789, "new user channer id")
	ts := flag.Int("ts", 1, "ts")
	te := flag.Int("te", 1, "te")
	gameUin := flag.String("gameUin", "000A8173C81B7395", "game_uin")
	device_id := flag.String("device_id", "jiebinllllllx", "device_id")
	remark := flag.String("remark", "flaaaaaminnnngggo", "remark")
	setType := flag.String("setType", "1001,1002,1003", "setType")
	subcmd := flag.Int("subcmd", 1, "subcmd : 1. GetAppGameUinReq 3. GetUidFromGameUinReq 5. GetUidAllAppidAndGameUinReq 7. GetUcidAllAppidAndGameUinReq 9.GetAppGameUinWithoutReq 11.UpdateUcidReq")
	flag.Parse()
	thriftClient := thriftclient.ThriftBaseClientPool{}
	var paddr base.ServerAddr
	var addrs []base.ServerAddr
	paddr.IP = *host
	paddr.PORT = *port
	paddr.TIMEOUT = 10
	addrs = append(addrs, paddr)
	thriftClient.SetClientAddrs(addrs)
	thriftClient.PoolConnectInit(10, 10, time.Duration(100)*time.Second, true)
	var protoSetType []uint32
	setArray := strings.Split(*setType, ",")
	for _, stype := range setArray {
		intstype, err := strconv.Atoi(stype)
		if err != nil {
			fmt.Println("stype 不为数字")
			return
		}
		protoSetType = append(protoSetType, uint32(intstype))
	}
	info := &AppidAndGameUin{
		Uid:proto.Uint64(uint64(*uid)),
		Appid:proto.Uint64(uint64(*appid)),
		GameUin:proto.String(*gameUin),
		Cid:proto.Uint64(uint64(*cid)),
		Ucid:proto.Uint64(uint64(*ucid)),
		Remark:proto.String(*remark),
	}
	resProto := &SXXGameUinProto{}
	reqProto := &SXXGameUinProto{
		Result: proto.Int32(1),
		//Subcmd: proto.Int32(int32(SXXGameUinProto_SUBCMD_SUBCMD_SXXGameUinProto_GETAPPGAMEUINREQ)),
		Subcmd: proto.Int32(int32(*subcmd)),
	}
	switch *subcmd {
	case 1:
		getAppGameUinReq := &GetAppGameUinReq{
			Appid: proto.Uint64(uint64(*appid)),
			Uid:   proto.Uint64(uint64(*uid)),
			Cid:   proto.Uint64(uint64(*cid)),
		}
		reqProto.GetAppGameUinReq = getAppGameUinReq
	case 3:
		getUidFromGameUinReq := &GetUidFromGameUinReq{
			Appid:   proto.Uint64(uint64(*appid)),
			GameUin: proto.String(*gameUin),
		}
		reqProto.GetUidFromGameUinReq = getUidFromGameUinReq
	case 5:
		getUidAllAppidAndGameUinReq := &GetUidAllAppidAndGameUinReq{
			Uid: proto.Uint64(uint64(*uid)),
		}
		reqProto.GetUidAllAppidAndGameUinReq = getUidAllAppidAndGameUinReq
	case 7:
		getUcidAllAppidAndGameUinReq := &GetUcidAllAppidAndGameUinReq{
			Ucid: proto.Uint64(uint64(*ucid)),
			TS:   proto.Uint64(uint64(*ts)),
			TE:   proto.Uint64(uint64(*te)),
		}
		reqProto.GetUcidAllAppidAndGameUinReq = getUcidAllAppidAndGameUinReq
	case 9:
		getAppGameUinWithoutCreateReq := &GetAppGameUinWithoutCreateReq{
			Appid: proto.Uint64(uint64(*appid)),
			Uid:   proto.Uint64(uint64(*uid)),
		}
		reqProto.GetAppGameUinWithoutCreateReq = getAppGameUinWithoutCreateReq
	case 11:
		updateUcidReq := &UpdateUcidReq{
			Uid:     proto.Uint64(uint64(*uid)),
			Appid:   proto.Uint64(uint64(*appid)),
			NewUcid: proto.Uint64(uint64(*new_ucid)),
		}
		reqProto.UpdateUcidReq = updateUcidReq
	case 13:
		createSubGameUinReq := &CreateSubGameUinReq{
			Uid:	proto.Uint64(uint64(*uid)),
			Appid:	proto.Uint64(uint64(*appid)),
			Remark:proto.String(string(*remark)),
		}
		reqProto.CreateSubGameUinReq = createSubGameUinReq
	case 15:
		getUidAndAppidAllGameUinReq := &GetUidAndAppidAllGameUinReq{
			Uid: proto.Uint64(uint64(*uid)),
			Appid:proto.Uint64(uint64(*appid)),
		}
		reqProto.GetUidAndAppidAllGameUinReq = getUidAndAppidAllGameUinReq
	case 17:
		clearGameUinOrLoginTimeCacheReq := &ClearGameUinOrLoginTimeCacheReq{
			Uid: proto.Uint64(uint64(*uid)),
			Appid:proto.Uint64(uint64(*appid)),
			Type: proto.Uint64(1),
		}
		reqProto.ClearGameUinOrLoginTimeCacheReq = clearGameUinOrLoginTimeCacheReq
	case 19:
		updateGameUinReq := &UpdateGameUinReq{
			SetType: protoSetType,
			Info: info,
		}
		reqProto.UpdateGameUinReq = updateGameUinReq
	case 21:
		getLoginLogReq := &GetLoginLogReq{
			Appid: proto.Uint64(uint64(*appid)),
			Uid:proto.Uint64(uint64(*uid)),
		}
		reqProto.GetLoginLogReq = getLoginLogReq
	}
	fmt.Println(reqProto)
	resPkg := &XXUnitySSPkg{}
	reqPkg := &XXUnitySSPkg{}
	userInfo := &UserInfo{
		Uuid:      proto.String(*device_id),
		Version:   proto.String("2.5.0"),
		ProductID: ProductID_PI_XXAppStore.Enum(),
		IDFA:      proto.String("jjjjjeeeebbbbiiinnlllllxxxx"),
	}
	serverHead := &XXUnitySSPkgHead{
		InterfaceIp:   proto.Uint32(0),
		InterfacePort: proto.Uint32(0),
		ClientIp:      proto.Uint32(1921686111),
		ClientPort:    proto.Uint32(0),
		Flow:          proto.Uint64(0),
		PlatformType:  PlatformType_PT_None.Enum(),
	}
	clientHead := &XXUnityCSPkgHead{
		Cmd:      proto.Uint32(uint32(SXXGameUinProto_CMD_CMD_SXXGameUinProto)),
		UserInfo: userInfo,
	}
	reqPkg.ClientHead = clientHead
	reqPkg.ServerHead = serverHead
	body, err := proto.Marshal(reqProto)
	if err != nil {
		fmt.Println(err)
	}
	reqPkg.Body = body
	reqBody, err := proto.Marshal(reqPkg)
	if err != nil {
		fmt.Println(err)
	}
	sendReq := &tbase.BinDataReq{
		Req: reqBody,
	}
	sendRes, _ := thriftClient.Any(sendReq)
	if sendRes == nil {
		fmt.Println("send fail")
	}
	if sendRes.GetStatus() != 0 {
		fmt.Println("status!=0")
	}
	err = proto.Unmarshal(sendRes.GetRes(), resPkg)
	if err != nil {
		fmt.Println("ri0")
		fmt.Println(err)
	}
	err = proto.Unmarshal(resPkg.Body, resProto)
	if err != nil {
		fmt.Println("ri")
		fmt.Println(err)
	}
	jRes, _ := json.Marshal(resProto)
	fmt.Println(string(jRes))
}
