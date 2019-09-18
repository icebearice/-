package gp_game_uin

//#cgo LDFLAGS: -L/home/taurus/go_workspace/monitor/build64_release/monitor/oss/c -loss_attr_api
//#include "/home/taurus/go_workspace/monitor/oss/c/oss_attr_api.h"
import "C"

import (
	"database/sql"
	"encoding/json"
	"flamingo/logger"
	"flamingo/mysql"
	"fmt"
	"strconv"
)

type GPDevMysqlReaderHandler struct {
	mysql.MysqlBaseHandler
	worker *GPGameUinWorker
}

func (self *GPDevMysqlReaderHandler) getMaxappid() (uint64, error) {
	var maxAppid int
    C.OssAttrInc(159, 9, 1)
	SQL := fmt.Sprintf("SELECT id FROM app ORDER BY id DESC LIMIT 1")
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	defer rows.Close()
	data, err := self.ParseResultToString(rows)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return 0, err
	}
	for _, m := range data {
		maxAppid, err = strconv.Atoi(m["id"])
	}
	return uint64(maxAppid), nil
}

func (self *GPDevMysqlReaderHandler) getAppIds() ([]uint64, error) {
	var appids []uint64
	C.OssAttrInc(159, 9, 1)
	SQL := fmt.Sprintf("SELECT id from app ")
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
		_appid, _ := strconv.Atoi(m["id"])
		appids = append(appids, uint64(_appid))
	}
	return appids, nil
}

func (self *GPDevMysqlReaderHandler) getAllThirdAppIdMapBySource(source string) ([]AppIdThirdMap, error) {
	C.OssAttrInc(159, 10, 1)
	SQL := fmt.Sprintf(`
		SELECT 
			appid,
			3rd,
			3rd_appid,
			package
		FROM
			app_3rd_map
		WHERE
			3rd = '%s' 
		`, source)
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
	var appIdThirdMapArr []AppIdThirdMap
	for _, m := range data {
		appid, _ := strconv.Atoi(m["appid"])
		tmpAppIdThirdMap := AppIdThirdMap{
			Appid:       uint64(appid),
			Package:     m["package"],
			ThirdAppid:  m["3rd_appid"],
			ThirdSource: source,
		}
		appIdThirdMapArr = append(appIdThirdMapArr, tmpAppIdThirdMap)
	}
	return appIdThirdMapArr, nil
}

func (self *GPDevMysqlReaderHandler) getALLBlockAppids() []uint64 {
	var result []uint64
	C.OssAttrInc(159, 11, 1)
	SQL := fmt.Sprintf(`
		SELECT
			appid
		FROM
			app_gameuin_block
		WHERE
			status = 1
		`)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var appid sql.NullInt64
	for rows.Next() {
		rows.Scan(&appid)
		if appid.Int64 > 0 {
			result = append(result, uint64(appid.Int64))
		}
	}
	return result
}

func (self *GPDevMysqlReaderHandler) getMixAppIds() [][]uint64 {
	var result [][]uint64
	C.OssAttrInc(159, 12, 1)
	SQL := fmt.Sprintf(`
		SELECT
			appid_android,appid_ios
		FROM
			app_gameuin_mix
		WHERE
			status = 1
		`)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var appidAndroid, appidIos sql.NullInt64
	for rows.Next() {
		rows.Scan(&appidAndroid, &appidIos)
		result = append(result, []uint64{uint64(appidAndroid.Int64), uint64(appidIos.Int64)})
	}
	return result
}

func (self *GPDevMysqlReaderHandler) getAppidInfo(appid uint64) AppidInfo {
	var result AppidInfo
	C.OssAttrInc(159, 13, 1)
	SQL := fmt.Sprintf("SELECT id,platform FROM app where id = %d", appid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var id, platformId sql.NullInt64
	for rows.Next() {
		rows.Scan(&id, &platformId)
		result = AppidInfo{
			Appid:      uint64(id.Int64),
			PlatformId: uint64(platformId.Int64),
		}
	}
	return result
}

func (self *GPDevMysqlReaderHandler) getThirdDeviceInfo(deviceId string, appid uint64) ThirdDeviceInfo {
	var result ThirdDeviceInfo
	C.OssAttrInc(159, 14, 1)
	SQL := fmt.Sprintf(`
		SELECT
			device_id,pid,appid
		FROM
			third_device
		WHERE
			device_id = '%s' AND
			appid = %d
		limit 1`, deviceId, appid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var _appid, pid sql.NullInt64
	var _deviceId sql.NullString
	for rows.Next() {
		rows.Scan(&_deviceId, &pid, &_appid)
		result.Appid = uint64(_appid.Int64)
		result.Pid = uint64(pid.Int64)
		result.DeviceID = _deviceId.String
	}
	return result
}

func (self *GPDevMysqlReaderHandler) getChannelInfo(cid uint64) ChannelInfo {
	var result ChannelInfo
	C.OssAttrInc(159, 15, 1)
	SQL := fmt.Sprintf(`
		SELECT
			cid,reid,is_self
		FROM
			channel
		WHERE
			cid = %d
		`, cid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var _cid, reid, isSelf sql.NullInt64
	for rows.Next() {
		rows.Scan(&_cid, &reid, &isSelf)
		result.Cid = uint64(_cid.Int64)
		result.Reid = uint64(reid.Int64)
		result.IsSelf = uint64(isSelf.Int64)
	}
	return result
}

func (self *GPDevMysqlReaderHandler) getAppChargeBlockInfo(appid uint64) AppChargeBlockInfo {
	var result AppChargeBlockInfo
	C.OssAttrInc(159, 16, 1)
	SQL := fmt.Sprintf(`
		SELECT
			appid,block_info
		FROM
			app_recharge_block
		WHERE
			appid = %d
		`, appid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var _appid sql.NullInt64
	var blockInfoStr sql.NullString
	for rows.Next() {
		rows.Scan(&_appid, &blockInfoStr)
		result.Appid = uint64(_appid.Int64)
		var blockInfo BlockInfo
		json.Unmarshal([]byte(blockInfoStr.String), &blockInfo)
		result.BlockInfo = blockInfo
	}
	return result
}

func (self *GPDevMysqlReaderHandler) checkChannelBlockCreate(appid uint64, ucid uint64) bool {
	result := false
	C.OssAttrInc(159, 17, 1)
	SQL := fmt.Sprintf("SELECT block_type from app_channel_reg_recharge_block WHERE appid = %d AND cid = %d limit 1", appid, ucid)
	stmt, err := self.Conn.Prepare(SQL)
	if err != nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil || rows == nil {
		logger.Logln(logger.ERROR, err)
		return result
	}
	defer rows.Close()
	var blockType sql.NullInt64
	for rows.Next() {
		rows.Scan(&blockType)
		if blockType.Int64 == int64(0) {
			return true
		}
	}
	return result
}
