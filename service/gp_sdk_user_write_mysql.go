package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"flamingo/gpfile"
	"flamingo/logger"
	"flamingo/mysql"
	"fmt"
	XXProto "go/XXProtocols"
	"time"
)

type GPSDKUserMysqlWriterHandler struct {
	mysql.MysqlBaseHandler
	worker *GPGameUinWorker
}

func (self *GPSDKUserMysqlWriterHandler) insertSubGameUin(uid uint64, appid uint64, gameUin string, cid uint64, ucid uint64, uuid, ip, remark string) (bool, error) {
	now := time.Now().Unix()
	idx := self.worker.utilsManager.getAppidIdx(appid)
	SQL := fmt.Sprintf("INSERT INTO gamesdk_uin_%d (`uid`, `appid`, `game_uin`, `ucid`, `cid`,`addtime`,`uuid`,`ip`,`remark`) VALUES (?,?,?,?,?,?,?,?,?)", idx)
	logger.Logln(logger.DEBUG, SQL)
	logger.Logln(logger.DEBUG, fmt.Sprintf("uid:%d appid:%d gameuin:%s, ucid:%d cid :%d now:%d uuid:%s ip:%s remark:%s", uid, appid, gameUin, ucid, cid, now, uuid, ip, remark))
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		C.OssAttrInc(110, 15, 1)
		str := fmt.Sprintf("insert gameUin fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(uid, appid, gameUin, ucid, cid, now, uuid, ip, remark)
	C.OssAttrInc(110, 14, 1)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("insert gameUin fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		C.OssAttrInc(110, 15, 1)
		return false, err
	}
	var rowsaffected int64
	rowsaffected, _ = res.RowsAffected()
	if rowsaffected <= 0 {
		logger.Logln(logger.ERROR, err)
		C.OssAttrInc(110, 15, 1)
		return false, err
	}
	str := fmt.Sprintf("insert succeed:%s", SQL)
	self.logSomething(self.worker.manager.insertLogFile, str)
	logger.Logln(logger.DEBUG, str)
	C.OssAttrInc(110, 16, 1)
	return true, nil
}

func (self *GPSDKUserMysqlWriterHandler) insertGameUin(uid uint64, appid uint64, gameUin string, cid uint64, ucid uint64, uuid string, ip string) (bool, error) {
	C.OssAttrInc(159, 6, 1)
	now := time.Now().Unix()
	idx := self.worker.utilsManager.getAppidIdx(appid)
	SQL := fmt.Sprintf("INSERT INTO gamesdk_uin_%d (`uid`, `appid`, `game_uin`, `ucid`, `cid`,`addtime`,`uuid`,`ip`, `remark`) VALUES (?,?,?,?,?,?,?,?,?)", idx)
    remark := "未命名"
	userInfo, err := self.worker.manager.thriftManager.getUserInfo(uid)
	if err == nil {
		remark = userInfo.GetBase().GetUname()
	}
	logger.Logln(logger.DEBUG, SQL)
	logger.Logln(logger.DEBUG, fmt.Sprintf("uid:%d appid:%d gameuin:%s, ucid:%d cid :%d now:%d uuid:%s ip:%s remark:%s", uid, appid, gameUin, ucid, cid, now, uuid, ip, remark))
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		C.OssAttrInc(110, 15, 1)
		str := fmt.Sprintf("insert gameUin fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(uid, appid, gameUin, ucid, cid, now, uuid, ip, remark)
	C.OssAttrInc(110, 14, 1)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("insert gameUin fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		C.OssAttrInc(110, 15, 1)
		return false, err
	}
	var rowsaffected int64
	rowsaffected, _ = res.RowsAffected()
	if rowsaffected <= 0 {
		C.OssAttrInc(110, 15, 1)
		return false, err
	}
	str := fmt.Sprintf("insert succeed:%s", SQL)
	self.logSomething(self.worker.manager.insertLogFile, str)
	logger.Logln(logger.DEBUG, str)
	C.OssAttrInc(110, 16, 1)
	return true, nil
}

func (self *GPSDKUserMysqlWriterHandler) updateUcid(uid uint64, appid uint64, gameUin string, newUcid uint64) (bool, error) {
	C.OssAttrInc(159, 8, 1)
	idx := self.worker.utilsManager.getAppidIdx(appid)
	SQL := fmt.Sprintf("update gamesdk_uin_%d set `ucid` = '%d' where `uid` = '%d' and `appid` = '%d' and `game_uin` = '%s'", idx, newUcid, uid, appid, gameUin)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("update ucid fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("update ucid fail: %s", SQL)
		C.OssAttrInc(110, 56, 1)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	var rowsaffected int64
	rowsaffected, _ = res.RowsAffected()
	if rowsaffected > 0 {
		str := fmt.Sprintf("update succeed:%s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		logger.Logln(logger.DEBUG, str)
		return true, nil
	}
	C.OssAttrInc(110, 55, 1)
	logger.Logln(logger.DEBUG, "rowsAffected:", rowsaffected)
	return true, nil
}

func (self *GPSDKUserMysqlWriterHandler) updateGameUin(index uint64, setType []uint32, info *SAppidAndGameUin) bool {
	if err := self.Ping(); err != nil {
		return false
	}
	table := fmt.Sprintf("gamesdk_uin_%d", index)
	SQL := fmt.Sprintf("UPDATE `%s` SET", table)
	var should_mark bool
	should_mark = false
	args := []interface{}{}
	for _, v := range setType {
		if v == uint32(XXProto.SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Remark) {
			if should_mark {
				SQL += fmt.Sprintf(", `remark` = ?")
			} else {
				SQL += fmt.Sprintf(" `remark` = ?")
			}
			should_mark = true
			args = append(args, info.Remark)
		} else if v == uint32(XXProto.SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Ucid) {
			if should_mark {
				SQL += fmt.Sprintf(", `ucid` = ?")
			} else {
				SQL += fmt.Sprintf(" `ucid` = ?")
			}
			should_mark = true
			args = append(args, info.Ucid)
		} else if v == uint32(XXProto.SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_Cid) {
			if should_mark {
				SQL += fmt.Sprintf(", `cid` = ?")
			} else {
				SQL += fmt.Sprintf(" `cid` = ?")
			}
			args = append(args, info.Cid)
			should_mark = true
		} else if v == uint32(XXProto.SXXGameUinProto_SETTYPE_SETTYPE_SXXGameUinProto_RchangeAmount) {
			if should_mark {
				SQL += fmt.Sprintf(", `recharge_amount` = `recharge_amount` + ?")
			} else {
				SQL += fmt.Sprintf(" `recharge_amount` = `recharge_amount` + ?")
			}
			args = append(args, info.RechargeAmount)
			should_mark = true
		}
	}
	SQL += fmt.Sprintf(" WHERE `appid` = ? AND `game_uin` = ?")
	args = append(args, info.Appid)
	args = append(args, info.GameUin)
	logger.Logln(logger.INFO, SQL)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return false
	}
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return false
	}
	if ra, _ := res.RowsAffected(); ra != 1 {
		logger.Logln(logger.ERROR, "exec affected rows != 1")
		return false
	}
	ra, _ := res.RowsAffected()
	logger.Logln(logger.DEBUG, ra)
	return true
}

func (self *GPSDKUserMysqlWriterHandler) logSomething(filename string, logstr string) {
	res, errf := gpfile.LogToFile(filename, logstr)
	logger.Logln(logger.DEBUG, res)
	logger.Logln(logger.DEBUG, errf)
}
