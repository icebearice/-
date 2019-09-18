package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
//import "C"

import (
	"errors"
	"flamingo/gpfile"
	"flamingo/logger"
	"flamingo/mysql"
	"fmt"
)

type GPDevMysqlWriterHandler struct {
	mysql.MysqlBaseHandler
	worker *GPGameUinWorker
}

func (self *GPDevMysqlWriterHandler) insertThirdProtect(info ThirdProtectInfo) (bool, error) {
	//C.OssAttrInc(159, 25, 1)
	SQL := fmt.Sprintf("INSERT INTO third_protect (`appid`, `third_partner_id`, `device_id`,`uin`) VALUES ('%d','%d','%s','%d')", info.Appid, info.ThirdPartnerID, info.DeviceID, info.Uin)
	logger.Logln(logger.DEBUG, SQL)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("insert thirdProtect fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		str := fmt.Sprintf("insert thirdProtect fail: %s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		return false, err
	}
	var rowsaffected int64
	rowsaffected, _ = res.RowsAffected()
	if rowsaffected <= 0 {
		res2, err := stmt.Exec()
		if err != nil {
			logger.Logln(logger.ERROR, err)
			return false, err
		}
		rowsaffected, _ = res2.RowsAffected()
		if rowsaffected <= 0 {
			str := fmt.Sprintf("insert thirdProtect fail: %s", SQL)
			self.logSomething(self.worker.manager.insertLogFile, str)
			logger.Logln(logger.ERROR, str)
			return false, errors.New(str)
		}
	}
	if rowsaffected > 0 {
		str := fmt.Sprintf("insert succeed:%s", SQL)
		self.logSomething(self.worker.manager.insertLogFile, str)
		logger.Logln(logger.DEBUG, str)
		return true, nil
	}

	return false, errors.New("insert thirdProtect fail ,rowsaffected weird")
}

func (self *GPDevMysqlWriterHandler) insertThirdDevice(info ThirdDeviceInfo) error {
	//C.OssAttrInc(159, 26, 1)
	tx, err := self.Conn.Begin()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return err
	}
	SQL := fmt.Sprintf("INSERT INTO third_device (`appid`, `pid`, `device_id`,`zuid`) VALUES ('%d','%d','%s','%d')", info.Appid, info.Pid, info.DeviceID, info.Zuid)
	logger.Logln(logger.DEBUG, SQL)
	stmt, err := tx.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		tx.Rollback()
		return err
	}
	er, err := result.RowsAffected()
	if err != nil || er < 0 {
		logger.Logln(logger.ERROR, err, er)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (self *GPDevMysqlWriterHandler) insertThirdGameUser(thirdGameUser ThirdGameUser) (int64, error) {
	var id int64
	//C.OssAttrInc(159, 27, 1)
	tx, err := self.Conn.Begin()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return id, err
	}
	SQL := fmt.Sprintf(`
		INSERT INTO 
			third_game_user (third_app_id, third_uin, third_username,game_uin,addtime,device_id,is_own,source_pid,ip) 
		VALUES 
			('%s','%s','%s','%s','%d','%s','%d','%d','%s')`,
		thirdGameUser.ThirdAppid, thirdGameUser.ThirdUin, thirdGameUser.ThirdUserName, thirdGameUser.GameUin, thirdGameUser.Addtime, thirdGameUser.DeviceId, thirdGameUser.IsOwn, thirdGameUser.SourcePid, thirdGameUser.Ip)
	logger.Logln(logger.DEBUG, SQL)
	stmt, err := tx.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		tx.Rollback()
		return id, err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		tx.Rollback()
		return id, err
	}
	er, err := result.RowsAffected()
	if err != nil || er < 0 {
		logger.Logln(logger.ERROR, err, er)
		tx.Rollback()
		return id, err
	}
	id, err = result.LastInsertId()
	tx.Commit()
	return id, err
}

func (self *GPDevMysqlWriterHandler) logSomething(filename string, logstr string) {
	res, errf := gpfile.LogToFile(filename, logstr)
	logger.Logln(logger.DEBUG, res)
	logger.Logln(logger.DEBUG, errf)
}
