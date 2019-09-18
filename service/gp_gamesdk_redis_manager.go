package gp_game_uin

import (
	"encoding/json"
	"flamingo/base"
	"flamingo/logger"
	"flamingo/xxredis"
	"fmt"
	"github.com/garyburd/redigo/redis"
    "errors"
)

type GPGameSdkRedisHandler struct {
	xxredis.RedisBaseHandler
}

type GPGameSdkRedisManager struct {
	hm  map[string]*GPGameSdkRedisHandler
	ttl int64
}

func (self *GPGameSdkRedisManager) SetRedisInfo(info base.RedisInfo) {
	if self.hm == nil {
		self.hm = make(map[string]*GPGameSdkRedisHandler)
	}
	if h, ok := self.hm[info.DESC]; ok {
		h.Close()
		logger.Logln(logger.ERROR, "found the same config and has cloesed the connection, pls check the config file on", info.DESC)
	}
	handler := &GPGameSdkRedisHandler{}
	handler.SetRedisInfo(info)
	err := handler.Init()
	if err == nil {
		logger.Logln(logger.DEBUG, "RedisBaseHandler connected")
	}
	self.hm[info.DESC] = handler
	if self.ttl == 0 {
		self.ttl = 3600 * 24 * 3
	}
}

func (self *GPGameSdkRedisManager) getKeyIndex(appid uint64, uid uint64) uint64 {
	return (uid/2 + 1) % 10
}

func (self *GPGameSdkRedisManager) getRedisHandler(key string) *GPGameSdkRedisHandler {
	h, ok := self.hm[key]
	if ok {
		return h
	}
	logger.Logln(logger.ERROR, "can not find the redis of key", key)
	return nil
}

func (self *GPGameSdkRedisManager) delCacheInfo(appid uint64, uid uint64) bool {
	key := fmt.Sprintf("gp_game_uin:%d_%d", appid, uid)
	handler := fmt.Sprintf("gp_game_uin_info_%d", self.getKeyIndex(appid, uid))
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return false
	}
	infos := self.getCacheInfo(appid, uid)
	if infos == nil {
		return true
	}
	for i := 0; i < 3; i++ {
		_, err := redis.Int64(h.Do("DEL", key))
		if err != nil && err != redis.ErrNil {
			logger.Logln(logger.ERROR, err)
			return false
		}
		return true
	}
	logger.Logln(logger.ERROR, "try 3 times,del cache error!")
	return false
}

func (self *GPGameSdkRedisManager) delLoginGameUin(appid uint64, uid uint64) bool {
	key := fmt.Sprintf("appid_%d_uid_%d", appid, uid)
	idx := self.getKeyIndex(appid, uid)
	handler := fmt.Sprintf("gp_game_uin_login_%d", idx)
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return false
	}
	gameUinLogin := self.getLoginGameUinByDesc(appid, uid)
	if gameUinLogin == nil {
		logger.Logln(logger.DEBUG, "not have cache for login_time: ", handler)
		return true
	}
	for i := 0; i < 3; i++ {
		_, err := redis.Int64(h.Do("DEL", key))
		if err != nil && err != redis.ErrNil {
			logger.Logln(logger.ERROR, err)
			return false
		}
		return true
	}
	logger.Logln(logger.ERROR, "try 3 times,del cache error!")
	return false
}

func (self *GPGameSdkRedisManager) getLoginGameUinByDesc(appid uint64, uid uint64) []*GameUinLogin {
	key := fmt.Sprintf("appid_%d_uid_%d", appid, uid)
	idx := self.getKeyIndex(appid, uid)
	handler := fmt.Sprintf("gp_game_uin_login_%d", idx)
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return nil
	}
	bytes, err := redis.Bytes(h.Do("GET", key))
	if err != nil && err != redis.ErrNil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	if bytes == nil {
		logger.Logln(logger.DEBUG, "can't find login_time cache in redis")
		return nil
	}
	var gameUinLoginTime []*GameUinLogin
	err = json.Unmarshal(bytes, &gameUinLoginTime)
	if err != nil {
		logger.Logln(logger.ERROR, "unmarshal error!")
		return nil
	}
	return gameUinLoginTime
}

func (self *GPGameSdkRedisManager) setLoginGameUinByDesc(appid uint64, uid uint64, gameUinLogin []*GameUinLogin) {
	key := fmt.Sprintf("appid_%d_uid_%d", appid, uid)
	idx := self.getKeyIndex(appid, uid)
	handler := fmt.Sprintf("gp_game_uin_login_%d", idx)
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return
	}
	if gameUinLogin == nil {
		logger.Logln(logger.ERROR, "gameUinLogin info is nil,set cache error")
		return
	}
	byte, err := json.Marshal(gameUinLogin)
	if err != nil {
		logger.Logln(logger.ERROR, "marshal game_uin_info error")
		return
	}
	for i := 0; i < 3; i++ {
		_, err := h.Do("SET", key, byte, "EX", 60*60*2)
		if err == nil {
			return
		}
	}
	logger.Logln(logger.ERROR, "try 3 times,set cache error!")
}

func (self *GPGameSdkRedisManager) getCacheInfo(appid uint64, uid uint64) []*SAppidAndGameUin {
	key := fmt.Sprintf("gp_game_uin:%d_%d", appid, uid)
	handler := fmt.Sprintf("gp_game_uin_info_%d", self.getKeyIndex(appid, uid))
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return nil
	}
	expires, err := redis.Bytes(h.Do("GET", key))
	if err != nil && err != redis.ErrNil {
		logger.Logln(logger.ERROR, err)
		return nil
	}
	if expires == nil {
		logger.Logln(logger.DEBUG, "can't find cache in redis")
		return nil
	}
	var infos []*SAppidAndGameUin
	err = json.Unmarshal(expires, &infos)
	if err != nil {
		logger.Logln(logger.ERROR, "unmarshal error!")
		return nil
	}
	return infos
}

func (self *GPGameSdkRedisManager) setCacheInfo(appid uint64, uid uint64, info []*SAppidAndGameUin) bool {
	key := fmt.Sprintf("gp_game_uin:%d_%d", appid, uid)
	handler := fmt.Sprintf("gp_game_uin_info_%d", self.getKeyIndex(appid, uid))
	h := self.getRedisHandler(handler)
	if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return false
	}
	if info == nil {
		logger.Logln(logger.ERROR, "info is nil,set cache error")
		return false
	}
	data, err := json.Marshal(info)
	if err != nil {
		logger.Logln(logger.ERROR, "marshal game_uin_info error")
		return false
	}
	for i := 0; i < 3; i++ {
		_, err := h.Do("SET", key, data, "EX", 86400*3)
		if err == nil {
			return true
		}
	}
	logger.Logln(logger.ERROR, "try 3 times,set cache error!")
	return false
}

func (self *GPGameSdkRedisManager) getAllGameUin(uid, index uint64) ([]*SAppidAndGameUin, error) {
    handler := fmt.Sprintf("gp_game_uin_dev") 
	h := self.getRedisHandler(handler)
    if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return nil, errors.New("not found redis handler")
    }
    key := fmt.Sprintf("uid_appids_%d_%d", uid, index)
    data, err := redis.Bytes(h.Do("GET", key))
    if err != nil {
        logger.Logln(logger.ERROR, err)
        return nil, err
    }
    if data == nil {
        return nil, nil
    }
    var res []*SAppidAndGameUin
    err = json.Unmarshal(data, &res)
    if err != nil {
        logger.Logln(logger.ERROR, err)
        return nil, err
    }
    return res, nil
}

func (self *GPGameSdkRedisManager) setAllGameUin(uid, index uint64, infos []*SAppidAndGameUin) {
    handler := fmt.Sprintf("gp_game_uin_dev") 
	h := self.getRedisHandler(handler)
    if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return
    }
    key := fmt.Sprintf("uid_appids_%d_%d", uid, index)
    data, err := json.Marshal(infos)
    if err != nil {
		logger.Logln(logger.ERROR, "marshal game_uin_info error")
		return
    }
    _, err = redis.Bytes(h.Do("SET", key, data, "EX", 86400*3))
    if err != nil && err != redis.ErrNil {
        logger.Logln(logger.ERROR, err)
    }
    return
}

func (self *GPGameSdkRedisManager) delAllGameUin(uid, index uint64) {
    handler := fmt.Sprintf("gp_game_uin_dev") 
	h := self.getRedisHandler(handler)
    if h == nil {
		logger.Logln(logger.ERROR, "can not get redis handler: ", handler)
		return
    }
    key := fmt.Sprintf("uid_appids_%d_%d", uid, index)
    _, err := redis.Bytes(h.Do("DEL", key))
    if err != nil && err != redis.ErrNil {
        logger.Logln(logger.ERROR, err)
    }
    return
}
