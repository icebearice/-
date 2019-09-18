package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"flamingo/logger"
	"flamingo/mysql"
	"fmt"
	"github.com/golang/protobuf/proto"
	. "go/XXProtocols"
	"strconv"
	"strings"
)

type GPSDKUserMysqlReaderHandler struct {
	mysql.MysqlBaseHandler
	worker *GPGameUinWorker
}

func (self *GPSDKUserMysqlReaderHandler) Ping() error {
	var err error
	if err = self.Conn.Ping(); err != nil {
		logger.Logln(logger.ERROR, "ping", err)
	}
	return err
}

func (self *GPSDKUserMysqlReaderHandler) getLoginIndex(uid uint64) uint64 {
	return (uid/2 + 1) % 10
}

func (self *GPSDKUserMysqlReaderHandler) checkExist(uid uint64, appid uint64, remark string) bool {
	idx := self.worker.utilsManager.getAppidIdx(appid)
	SQL := fmt.Sprintf("SELECT remark from gamesdk_uin_%d where uid = %d and appid = %d and remark = '%s' ", idx, uid, appid, remark)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return false
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return false
	}
	defer rows.Close()
	if rows.Next() {
		logger.Logln(logger.ERROR, "remark repeated")
		return false
	}
	return true
}

func (self *GPSDKUserMysqlReaderHandler) getSubGameUin(appid uint64, uid uint64) []*SAppidAndGameUin {
	C.OssAttrInc(159, 7, 1)
	if err := self.Ping(); err != nil {
		return nil
	}
	var infos []*SAppidAndGameUin
	idx := self.worker.utilsManager.getAppidIdx(appid)
	logger.Logln(logger.DEBUG, "idx=", idx)
	tableWithIdx := fmt.Sprintf("gamesdk_uin_%d", idx)
	SQL := fmt.Sprintf("SELECT uid, appid, game_uin, addtime, cid, ucid, ip, uuid, remark, recharge_amount FROM %s WHERE uid = %d  AND appid = %d ORDER BY `addtime` DESC",
		tableWithIdx, uid, appid)
	logger.Logln(logger.DEBUG, SQL)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	if rows == nil {
		logger.Logln(logger.ERROR, "rows is nil")
		return nil
	}
	defer rows.Close()
	data, err := self.ParseResultToString(rows)
	for _, m := range data {
		gameUinStr := m["game_uin"]
		if strings.Count(gameUinStr, "_") < 2 { //一个下划线以下属于渠道迁移，得取substring
			if subpos := strings.Index(gameUinStr, "_"); subpos > 0 {
				gameUinStr = gameUinStr[0:subpos]
			}
			if strings.Contains(m["game_uin"], "LL_") {
				gameUinStr = m["game_uin"]
			}
		}
		addtime, _ := strconv.Atoi(m["addtime"])
		cid, _ := strconv.Atoi(m["cid"])
		ucid, _ := strconv.Atoi(m["ucid"])
		remark := m["remark"]
		rechargeAmount, _ := strconv.Atoi(m["recharge_amount"])
		info := &SAppidAndGameUin{}
		info.Uid = uint64(uid)
		info.Appid = uint64(appid)
		info.GameUin = string(gameUinStr)
		info.Addtime = uint64(addtime)
		info.Cid = uint64(cid)
		info.Ucid = uint64(ucid)
		info.Remark = string(remark)
		info.RechargeAmount = uint64(rechargeAmount)
		infos = append(infos, info)
	}
	return infos
}

func (self *GPSDKUserMysqlReaderHandler) getLoginGameUinByDesc(appid uint64, uid uint64) []*GameUinLogin {
	idx := self.getLoginIndex(uid)
	SQL := fmt.Sprintf("SELECT game_uin, login_time FROM user_game_login_%d WHERE uid = %d AND appid = %d ORDER BY login_time DESC", idx, uid, appid)
	C.OssAttrInc(159, 7, 1)
	logger.Logln(logger.DEBUG, SQL)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	defer rows.Close()
	data, err := self.ParseResultToString(rows)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	var gameUinLogin []*GameUinLogin
	for _, m := range data {
		loginTime, _ := strconv.Atoi(m["login_time"])
		info := &GameUinLogin{
			Appid:     appid,
			Uid:       uid,
			GameUin:   m["game_uin"],
			LoginTime: uint64(loginTime),
		}
		gameUinLogin = append(gameUinLogin, info)
	}
	return gameUinLogin
}

func (self *GPSDKUserMysqlReaderHandler) getGameUin(appid uint64, uid uint64) (*AppidAndGameUin, error) {
	//C.OssAttrInc(159, 7, 1)
	idx := self.worker.utilsManager.getAppidIdx(appid)
	tableWithIdx := fmt.Sprintf("gamesdk_uin_%d", idx)
	SQL := fmt.Sprintf(`
		SELECT 
			game_uin,
			uid,
			appid,
			addtime,
			cid,
			ucid,
			remark,
			recharge_amount
		FROM %s
		WHERE 
			uid = %d AND
			appid = %d
		LIMIT 1
		`, tableWithIdx, uid, appid)
	stmt1, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer stmt1.Close()
	rows1, err := stmt1.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer rows1.Close()
	data1, err := self.ParseResultToString(rows1)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	gameUinInfo := &AppidAndGameUin{}
	for _, m1 := range data1 {
		gameUinStr := m1["game_uin"]
		if strings.Count(gameUinStr, "_") < 2 { //一个下划线以下属于渠道迁移，得取substring
			if subpos := strings.Index(gameUinStr, "_"); subpos > 0 {
				gameUinStr = gameUinStr[0:subpos]
			}
			if strings.Contains(m1["game_uin"], "LL_") {
				gameUinStr = m1["game_uin"]
			}
		}
		addtime, _ := strconv.Atoi(m1["addtime"])
		cid, _ := strconv.Atoi(m1["cid"])
		ucid, _ := strconv.Atoi(m1["ucid"])
		rechargeAmount, _ := strconv.Atoi(m1["recharge_amount"])
		gameUinInfo.GameUin = proto.String(gameUinStr)
		gameUinInfo.Uid = proto.Uint64(uid)
		gameUinInfo.Appid = proto.Uint64(appid)
		gameUinInfo.Cid = proto.Uint64(uint64(cid))
		gameUinInfo.Ucid = proto.Uint64(uint64(ucid))
		gameUinInfo.Addtime = proto.Uint64(uint64(addtime))
		gameUinInfo.Remark = proto.String(m1["remark"])
		gameUinInfo.RechargeAmount = proto.Uint64(uint64(rechargeAmount))
	}
	return gameUinInfo, nil
}

func (self *GPSDKUserMysqlReaderHandler) getUidFromGameUin(appid uint64, gameUin string) (uint64, error) {
	var uid uint64
	//C.OssAttrInc(159, 18, 1)
	idx := self.worker.utilsManager.getAppidIdx(appid)
	tableWithIdx := fmt.Sprintf("gamesdk_uin_%d", idx)
	SQL := fmt.Sprintf(`
		SELECT uid
		FROM %s
		WHERE 
			game_uin = '%s' AND
			appid = %d
		LIMIT 1
		`, tableWithIdx, gameUin, appid)
	stmt1, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	defer stmt1.Close()
	rows1, err := stmt1.Query()
	//logger.Logln(logger.DEBUG, SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	defer rows1.Close()
	data1, err := self.ParseResultToString(rows1)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	for _, m1 := range data1 {
		_uid, _ := strconv.Atoi(m1["uid"])
		uid = uint64(_uid)
	}
	return uid, nil
}

func (self *GPSDKUserMysqlReaderHandler) getUcidAllAppidAndGameUin(ucid uint64, starttime uint64, endtime uint64, appids []uint64) ([]SAppidAndGameUin, error) {
	var SAppidAndGameUinArr []SAppidAndGameUin
	appidTbMap := make(map[uint64][]uint64)
	C.OssAttrInc(159, 20, 1)
	for _, appid := range appids {
		idx := self.worker.utilsManager.getAppidIdx(appid)
		appidTbMap[idx] = append(appidTbMap[idx], appid)
	}
	for idx, _appids := range appidTbMap {
		tmpSAppidAndGameUinArr, err := self.getGameUinsByUcidWithIdx(ucid, starttime, endtime, idx, _appids)
		if err != nil {
			logger.Logln(logger.ERROR, err)
			return nil, err
		}
		SAppidAndGameUinArr = append(SAppidAndGameUinArr, tmpSAppidAndGameUinArr...)
	}
	return SAppidAndGameUinArr, nil
}

func (self *GPSDKUserMysqlReaderHandler) getGameUinsByUid(uid uint64, idx uint64) ([]*SAppidAndGameUin, error) {
	var SAppidAndGameUinArr []*SAppidAndGameUin
	SQL := fmt.Sprintf("SELECT * from gamesdk_uin_%d WHERE uid = %d", idx, uid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer rows.Close()
	data, err := self.ParseResultToString(rows)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	for _, m := range data {
		_uid, _ := strconv.Atoi(m["uid"])
		_appid, _ := strconv.Atoi(m["appid"])
		_addtime, _ := strconv.Atoi(m["addtime"])
		_cid, _ := strconv.Atoi(m["cid"])
		_ucid, _ := strconv.Atoi(m["ucid"])
		_rechargeAmount, _ := strconv.Atoi(m["recharge_amount"])
		SAppidAndGameUinArr = append(SAppidAndGameUinArr, &SAppidAndGameUin{
			Uid:            uint64(_uid),
			Appid:          uint64(_appid),
			Addtime:        uint64(_addtime),
			Cid:            uint64(_cid),
			Ucid:           uint64(_ucid),
			GameUin:        m["game_uin"],
			Remark:         m["remark"],
			RechargeAmount: uint64(_rechargeAmount),
		})
	}
	return SAppidAndGameUinArr, nil
}

func (self *GPSDKUserMysqlReaderHandler) getGameUinsByUcidWithIdx(ucid uint64, starttime uint64, endtime uint64, idx uint64, appids []uint64) ([]SAppidAndGameUin, error) {
	tb := fmt.Sprintf("gamesdk_uin_%d", idx)
	C.OssAttrInc(159, 22, 1)
	var appidsStr string
	var SAppidAndGameUinArr []SAppidAndGameUin
	for _, appid := range appids {
		appidsStr += fmt.Sprintf("%d,", appid)
	}
	appidsStr += appidsStr + "0"
	SQL := fmt.Sprintf(`
		SELECT * 
		FROM %s 
		WHERE ucid = %d AND 
			  appid in (%s) AND
			  addtime BETWEEN '%d' AND '%d'
		`, tb, ucid, appidsStr, starttime, endtime)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	defer rows.Close()
	data, err := self.ParseResultToString(rows)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return nil, err
	}
	for _, m := range data {
		_uid, _ := strconv.Atoi(m["uid"])
		_appid, _ := strconv.Atoi(m["appid"])
		_addtime, _ := strconv.Atoi(m["addtime"])
		_cid, _ := strconv.Atoi(m["cid"])
		_ucid, _ := strconv.Atoi(m["ucid"])
		_rechargeAmount, _ := strconv.Atoi(m["recharge_amount"])
		tmpSAppidAndGameUin := SAppidAndGameUin{
			Uid:            uint64(_uid),
			Appid:          uint64(_appid),
			Addtime:        uint64(_addtime),
			Cid:            uint64(_cid),
			Ucid:           uint64(_ucid),
			GameUin:        m["game_uin"],
			RechargeAmount: uint64(_rechargeAmount),
			Remark:         m["remark"],
		}
		SAppidAndGameUinArr = append(SAppidAndGameUinArr, tmpSAppidAndGameUin)
	}

	return SAppidAndGameUinArr, nil
}
