package gp_game_uin

const (
	ISSELF_GUILD     uint64 = 0
	ISSELF_SELF      uint64 = 1
	ISSELF_CPA       uint64 = 2
	ISSELF_CPS       uint64 = 3
	ISSELF_OTHER1    uint64 = 6
	ISSELF_OTHER2    uint64 = 7
	GUILD_BLOCK_NO   string = "0"
	GUILD_BLOCK_ALL  string = "1"
	GUILD_BLOCK_PART string = "2"

	THIRDPART_IOS_PLATFORM     uint64 = 98
	THIRDPART_ANDROID_PLATFORM uint64 = 99
)

type AppIdThirdMap struct {
	Appid       uint64 `json:"appid"`
	ThirdSource string `json:"3rd"`
	ThirdAppid  string `json:"3rd_appid"`
	Package     string `json:"pakcage"`
}

type SAppidAndGameUin struct {
	Uid     uint64 `json:"uid"`
	Appid   uint64 `json:"appid"`
	GameUin string `json:"game_uin"`
	Addtime uint64 `json:"addtime"`
	Cid     uint64 `json:"cid"`
	Ucid    uint64 `json:"ucid"`
	Remark  string `json:"remark"`
	RechargeAmount   uint64 `json:"recharge_amount"`
    LoginTime uint64 `json:"login_time"`
}

type ThirdProtectInfo struct {
	Appid          uint64 `json:"appid"`
	ThirdPartnerID uint64 `json:"thrid_partner_id"`
	DeviceID       string `json:"device_id"`
	Uin            uint64 `json:"uin"`
}

type ThirdDeviceInfo struct {
	Appid    uint64 `json:"appid"`
	DeviceID string `json:"device_id"`
	Zuid     uint64 `json:"zuid"`
	Pid      uint64 `json:"pid"`
}

type WdjResponseInfo struct {
	StatusInfo WdjStatusInfo
	Data       WdjData
}

type WdjStatusInfo struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type WdjData struct {
	WdjUserId string `json:"wdjUserId"`
}

type PPTVResponseInfo struct {
	Status uint64   `json:"status"`
	Msg    string   `json:"message"`
	Data   PPTVData `json:"data"`
}

type PPTVData struct {
	UserId   string `json:"userid"`
	Username string `json:"username"`
	Token    string `json:"token"`
	Ext      string `json:"ext"`
}

type AppidInfo struct {
	Appid      uint64 `json:"appid"`
	PlatformId uint64 `json:"platform"`
}

type ThirdGameUser struct {
	ThirdAppid    string "$pid-$appid"
	ThirdUin      string "gp's uin"
	ThirdUserName string "gp's uname,not exit use uin"
	GameUin       string "our own gameUin"
	Addtime       uint64 "now.Unix"
	DeviceId      string "uuid"
	IsOwn         uint64
	SourcePid     uint64
	Ip            string "ip"
}

type GPUserInfo struct {
	Uid   uint64 "uin,uid"
	UName string "uname"
	Uex   string "uex"
	Ucid  uint64 "ucid"
}

type ChannelInfo struct {
	Cid    uint64 "channelId"
	Reid   uint64 "parent id"
	IsSelf uint64 "0:公会 1:自有 2:CPA 3:CPS 6,7:其他"
}

type AppChargeBlockInfo struct {
	Appid     uint64    "appid"
	BlockInfo BlockInfo "封停信息,具体转成json"
}

type BlockInfo struct {
	Appid             string            `json:"appid"`
	GameForbiddenRule GameForbiddenRule `json:"game_forbidden_rule"`
}

type GameForbiddenRule struct {
	GameSelf  string    `json:"game_self"`
	GameCpa   string    `json:"game_cpa"`
	GameCps   string    `json:"game_cps"`
	GameOther string    `json:"game_other"`
	GameGuild GameGuild `json:"game_guild"`
}

type GameGuild struct {
	All       string   `json:"all"`
	TopIdList []string `json:"top_id_list"`
}

type GameUinLogin struct {
	Appid     uint64 `json:"appid"`
	Uid       uint64 `json:"uin"`
	GameUin   string `json:"game_uin"`
	LoginTime uint64 `json:"login_time"`
}

type SAppidAndGameUins []*SAppidAndGameUin

type SortGameUin struct {
    SAppidAndGameUins
}
