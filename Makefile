BUILD_TIME=`date` 
BUILD_SVN=`svn info`
BUILD_GOVERSION = `go version`
out=$(notdir $(CURDIR))
src=gp_game_uin.go
.PHONY : build clean

default:clean build

build:
	        go build  -v -x -ldflags "-extldflags '-static' -X 'main.BUILD_TIME=${BUILD_TIME}' -X 'main.BUILD_SVN=${BUILD_SVN}' -X 'main.BUILD_GOVERSION=${BUILD_GOVERSION}'"  -o $(out) $(src) 
clean:
	        -rm $(out)

test:
	        make -C testdata runtest

scp:
	        scp -i ~/.ssh/id_rsa_chenchang -P 33335 xx_package_state.goc chenchang@jump.corp.flamingo-inc.com:~/
scp_aws:
	        scp -i ~/.ssh/flamingo_singapore.pem $(out) ec2-user@52.74.42.227:/tmp/
scp_pro:
	sshpass -p Fla_xysI_7121 scp -i ~/.ssh/id_rsa_xiaokang.jia -P 33335 ipa_user_favor_list xiaokang.jia@pxxzself01.rmz.flamingo-inc.com:/tmp/
	sshpass -p Fla_xysI_7121 scp -i ~/.ssh/id_rsa_xiaokang.jia -P 33335 ipa_user_favor_list xiaokang.jia@pxxzself02.rmz.flamingo-inc.com:/tmp/
