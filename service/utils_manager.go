package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flamingo/logger"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type UtilsManager struct {
	worker    *GPGameUinWorker
	manager   *GPGameUinManager
	cacheTime int64
}

func (self *UtilsManager) Init() {
	self.cacheTime = 600
}

func (self *UtilsManager) SetAppIdAndGameUin(SAppidAndGameUinArr []SAppidAndGameUin) []*AppidAndGameUin {
	var AppidAndGameUinArr []*AppidAndGameUin
	if len(SAppidAndGameUinArr) <= 0 {
		return AppidAndGameUinArr
	}
	for _, info := range SAppidAndGameUinArr {
		tmpAppidAndGameUin := &AppidAndGameUin{
			Uid:            proto.Uint64(info.Uid),
			Appid:          proto.Uint64(info.Appid),
			GameUin:        proto.String(info.GameUin),
			Addtime:        proto.Uint64(info.Addtime),
			Cid:            proto.Uint64(info.Cid),
			Ucid:           proto.Uint64(info.Ucid),
			Remark:         proto.String(info.Remark),
			RechargeAmount: proto.Uint64(info.RechargeAmount),
		}
		AppidAndGameUinArr = append(AppidAndGameUinArr, tmpAppidAndGameUin)
	}
	return AppidAndGameUinArr
}

func (self *UtilsManager) getAppidIdx(appid uint64) uint64 {
	ys := appid - 101100
	APPID_IDX_WIDTH := uint64(500)
	var idx uint64
	idx = ys / APPID_IDX_WIDTH
	return idx
}

func (self *UtilsManager) generateGameUin(appid uint64, uid uint64, ucid uint64) string {
	var gameUinStr string
	now := time.Now().Unix() + int64(rand.Intn(9999999))
	before := fmt.Sprintf("%d%d%d", uid, appid, now)
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(before))
	cipherStr := md5Ctx.Sum(nil)
	md5Str := hex.EncodeToString(cipherStr)
	logger.Logln(logger.DEBUG, md5Str)
	if len(md5Str) >= 0 {
		for _, v := range self.manager.ChangLLFirstList {
			if v == ucid {
				gameUinStr = "LL" + strings.ToUpper(string(md5Str[10:16+8]))
				return gameUinStr
			}
		}
		gameUinStr = strings.ToUpper(string(md5Str[8 : 16+8]))
	}
	return gameUinStr
}

func (self *UtilsManager) generateWdjUin(thirdAppid string) (string, error) {
	now := time.Now().Unix()
	toMd5 := fmt.Sprintf("%s%d%s", thirdAppid, now, self.manager.wdjUinKey)
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(toMd5))
	cipherStr := md5Ctx.Sum(nil)
	sign := hex.EncodeToString(cipherStr)
	url := fmt.Sprintf("%s?appid=%s&t=%d&sign=%s", self.manager.wdjUinUrl, thirdAppid, now, sign)
	logger.Logln(logger.DEBUG, "wdjUrl:"+url)
	res, err := http.Get(url)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return "", err
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return "", err
	}
	var wdjUin string
	wdjUin = string(robots)
	logger.Logln(logger.DEBUG, wdjUin)
	return wdjUin, nil
}

func (self *UtilsManager) generateWdjUinV2(thirdAppid string, uid uint64) (string, error) {
	now := time.Now().Unix()
	toMd52 := fmt.Sprintf("%s", self.manager.wdjUinKey)
	md5Ctx2 := md5.New()
	md5Ctx2.Write([]byte(toMd52))
	cipherStr2 := md5Ctx2.Sum(nil)
	keyMd5 := hex.EncodeToString(cipherStr2)
	toMd5 := fmt.Sprintf("appid=%s&outUserId=%d&platformId=%s&timeStamp=%d&%s", thirdAppid, uid, self.manager.wdjPlatformId, now, keyMd5)
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(toMd5))
	cipherStr := md5Ctx.Sum(nil)
	sign := hex.EncodeToString(cipherStr)
	url := fmt.Sprintf("%s?appid=%s&outUserId=%d&platformId=%s&timeStamp=%d&sign=%s",
		self.manager.wdjUinNewUrl, thirdAppid, uid, self.manager.wdjPlatformId, now, sign)
	logger.Logln(logger.DEBUG, "wdjUrlv2:"+url)
	res, err := http.Get(url)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return "", err
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return "", err
	}
	var wdjRes WdjResponseInfo
	err = json.Unmarshal(robots, &wdjRes)
	logger.Logln(logger.DEBUG, wdjRes)
	return wdjRes.Data.WdjUserId, err
}

func (self *UtilsManager) generatePPTVUin(thirdAppid string, uid uint64) PPTVResponseInfo {
	var result PPTVResponseInfo
	now := time.Now().Unix()
	key := self.manager.pptvUinKey
	toMd5 := fmt.Sprintf("gid=%s&outuid=%d&timestamp=%d%s", thirdAppid, uid, now, key)
	logger.Logln(logger.DEBUG, "beforemd5:", toMd5)
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(toMd5))
	cipherStr := md5Ctx.Sum(nil)
	sign := hex.EncodeToString(cipherStr)
	data := make(url.Values)
	data["gid"] = []string{thirdAppid}
	data["outuid"] = []string{fmt.Sprintf("%d", uid)}
	data["timestamp"] = []string{fmt.Sprintf("%d", now)}
	data["sign"] = []string{sign}
	url := fmt.Sprintf("%s", self.manager.pptvUinUrl)
	logger.Logln(logger.DEBUG, "pptv:"+url, data)
	res, err := http.PostForm(url, data)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	err = json.Unmarshal(robots, &result)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	logger.Logln(logger.DEBUG, result)
	return result
}

func (self *UtilsManager) getSpecialAppidMap(appid uint64) uint64 {
	specailAppIdMap := self.getMixAppIds()
	for _, m := range specailAppIdMap {
		if len(m) >= 2 {
			if appid == m[0] {
				return m[1]
			}
			if appid == m[1] {
				return m[0]
			}
		}
	}
	return 0
}

func (self *UtilsManager) getSecondBlockAppidMap(appid uint64) uint64 {
	if len(self.manager.blockSecondAppidMapArr) <= 0 {
		return 0
	}
	for _, m := range self.manager.blockSecondAppidMapArr {
		if len(m) >= 2 {
			if appid == m[1] {
				return m[0]
			}
		}
	}
	return 0
}

func (self *UtilsManager) getThirdWdjAppId(appid uint64) string {
	thirdAppIdMapArr, err := self.getAllThirdAppIdMapBySource("wdj")
	if err != nil {
		logger.Logln(logger.ERROR)
		return ""
	}
	if len(thirdAppIdMapArr) > 0 {
		for _, m := range thirdAppIdMapArr {
			if appid == m.Appid {
				return m.ThirdAppid
			}
		}
	}
	return ""
}

func (self *UtilsManager) getThirdPPTVAppId(appid uint64) string {
	pptvAppIdMapArr, err := self.getAllThirdAppIdMapBySource("pptv")
	if err != nil {
		logger.Logln(logger.ERROR)
		return ""
	}
	if len(pptvAppIdMapArr) > 0 {
		for _, m := range pptvAppIdMapArr {
			if appid == m.Appid {
				return m.ThirdAppid
			}
		}
	}
	return ""
}

func (self *UtilsManager) getAllThirdAppIdMapBySource(source string) ([]AppIdThirdMap, error) {
	var result []AppIdThirdMap
	key := fmt.Sprintf("get_all_third_appids_map_%s", source)
	handler := fmt.Sprintf("gp_game_uin_dev")
	h := self.manager.RedisManager.getRedisHandler(handler)
	if h != nil {
		expires, err := redis.Bytes(h.Do("GET", key))
		if err == redis.ErrNil || err == nil {
			err = json.Unmarshal(expires, &result)
			if err == nil {
				logger.Logln(logger.DEBUG, "get third appids maps from redis")
				return result, nil
			}
		}
	}
	result, err := self.worker.gpDevMysqlReaderHandler.getAllThirdAppIdMapBySource(source)
	if result == nil || err != nil {
		logger.Logln(logger.ERROR, "get third appids maps by mysql error ")
		return nil, err
	}
	if h != nil {
		jsonStr, _ := json.Marshal(result)
		_, err := h.Do("SET", key, jsonStr, "EX", self.cacheTime)
		if err != nil {
			logger.Logln(logger.ERROR, err)
		}
	} else {
		logger.Logln(logger.ERROR, "get redis handler error:", handler)
	}
	return result, nil
}

func (self *UtilsManager) getDevAppIds() ([]uint64, error) {
	var result []uint64
	key := fmt.Sprintf("get_dev_appids")
	handler := fmt.Sprintf("gp_game_uin_dev")
	h := self.manager.RedisManager.getRedisHandler(handler)
	if h != nil {
		expires, err := redis.Bytes(h.Do("GET", key))
		if err == redis.ErrNil || err == nil {
			err = json.Unmarshal(expires, &result)
			if err == nil {
				logger.Logln(logger.DEBUG, "get dev appids from redis")
				return result, nil
			}
		}
	}
	result, err := self.worker.gpDevMysqlReaderHandler.getAppIds()
	if result == nil || err != nil {
		logger.Logln(logger.ERROR, "get appids from app error ")
		return nil, err
	}
	if h != nil {
		jsonStr, _ := json.Marshal(result)
		_, err := h.Do("SET", key, jsonStr, "EX", self.cacheTime)
		if err != nil {
			logger.Logln(logger.ERROR, err)
		}
	} else {
		logger.Logln(logger.ERROR, "get redis handler error:", handler)
	}
	return result, nil
}

func (self *UtilsManager) getBlockAppIds() []uint64 {
	var result []uint64
	key := fmt.Sprintf("block_appids")
	handler := fmt.Sprintf("gp_game_uin_dev")
	h := self.manager.RedisManager.getRedisHandler(handler)
	if h != nil {
		expires, err := redis.Bytes(h.Do("GET", key))
		if err == redis.ErrNil || err == nil {
			err = json.Unmarshal(expires, &result)
			if err == nil {
				logger.Logln(logger.DEBUG, "get block appids from redis")
				return result
			}
		}
	}
	result = self.worker.gpDevMysqlReaderHandler.getALLBlockAppids()
	if h != nil {
		jsonStr, _ := json.Marshal(result)
		_, err := h.Do("SET", key, jsonStr, "EX", self.cacheTime*2)
		if err != nil {
			logger.Logln(logger.ERROR, err)
		}
	} else {
		logger.Logln(logger.ERROR, "get redis handler error:", handler)
	}
	return result
}

func (self *UtilsManager) getMixAppIds() [][]uint64 {
	var result [][]uint64
	key := fmt.Sprintf("mix_appids")
	handler := fmt.Sprintf("gp_game_uin_dev")
	h := self.manager.RedisManager.getRedisHandler(handler)
	if h != nil {
		expires, err := redis.Bytes(h.Do("GET", key))
		if err == redis.ErrNil || err == nil {
			err = json.Unmarshal(expires, &result)
			if err == nil {
				logger.Logln(logger.DEBUG, "get mix appids from redis")
				return result
			}
		}
	}
	result = self.worker.gpDevMysqlReaderHandler.getMixAppIds()
	if h != nil {
		jsonStr, _ := json.Marshal(result)
		_, err := h.Do("SET", key, jsonStr, "EX", self.cacheTime*6)
		if err != nil {
			logger.Logln(logger.ERROR, err)
		}
	} else {
		logger.Logln(logger.ERROR, "get redis handler error:", handler)
	}
	return result
}

func (self *UtilsManager) getThirdGameUser(appid uint64, pid uint64, uid uint64, gameUin string, uuid string, ip string) ThirdGameUser {
	var uname string
	gpUserInfo, err := self.worker.manager.thriftManager.getUserInfo(uid)
	if err == nil || gpUserInfo != nil {
		uname = gpUserInfo.Base.GetUname()
	}
	thirdDeviceInfo := self.worker.gpDevMysqlReaderHandler.getThirdDeviceInfo(uuid, appid)
	var isOwn, sourcePid uint64
	if len(uname) <= 0 {
		uname = fmt.Sprintf("%d", uid)
	}
	if len(thirdDeviceInfo.DeviceID) > 0 && uuid != "0f607264fc6318a92b9e13c65db7cd3c" && uuid != "Imei-GPApplication" {
		sourcePid = thirdDeviceInfo.Pid
		if thirdDeviceInfo.Pid == pid {
			isOwn = 1
		} else {
			isOwn = 0
		}
	} else {
		sourcePid = pid
		isOwn = 1
	}
	thirdGameUser := ThirdGameUser{
		ThirdAppid:    fmt.Sprintf("%d-%d", pid, appid),
		ThirdUin:      fmt.Sprintf("%d", uid),
		ThirdUserName: uname,
		GameUin:       gameUin,
		Addtime:       uint64(time.Now().Unix()),
		DeviceId:      uuid,
		IsOwn:         isOwn,
		SourcePid:     sourcePid,
		Ip:            ip,
	}
	return thirdGameUser
}

func (self *UtilsManager) getCidByUid(uid uint64) uint64 {
	uinfo, err := self.manager.thriftManager.getUserInfo(uid)
	if uinfo == nil || err != nil {
		logger.Logln(logger.ERROR, "not found any user data", uid)
		return 100
	}
	return uint64(uinfo.Ex.GetCid())
}

func (self *UtilsManager) checkBlockCreate(appid uint64, ucid uint64) bool {
	isChannelBlock := self.worker.gpDevMysqlReaderHandler.checkChannelBlockCreate(appid, ucid)
	if isChannelBlock == true {
		return true
	}
	appChargeBlockInfo := self.worker.gpDevMysqlReaderHandler.getAppChargeBlockInfo(appid)
	if appChargeBlockInfo.Appid > 0 {
		uChannelInfo := self.worker.gpDevMysqlReaderHandler.getChannelInfo(ucid)
		if uChannelInfo.Cid > 0 {
			switch uChannelInfo.IsSelf {
			case ISSELF_SELF:
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameSelf == "1" {
					return true
				}
			case ISSELF_CPA:
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameCpa == "1" {
					return true
				}
			case ISSELF_CPS:
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameCps == "1" {
					return true
				}
			case ISSELF_OTHER1, ISSELF_OTHER2:
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameOther == "1" {
					return true
				}
			case ISSELF_GUILD:
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameGuild.All == GUILD_BLOCK_NO {
					return false
				}
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameGuild.All == GUILD_BLOCK_ALL {
					return true
				}
				if appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameGuild.All == GUILD_BLOCK_PART {
					parentCid := appChargeBlockInfo.BlockInfo.GameForbiddenRule.GameGuild.TopIdList
					for _, v := range parentCid {
						_pid, _ := strconv.Atoi(v)
						if uint64(_pid) == uChannelInfo.Reid {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
