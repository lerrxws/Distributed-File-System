package utils

import (
	"fmt"

	"github.com/cihub/seelog"
)

func CreateLoggerWithPortNumber(port string) (seelog.LoggerInterface, error) {
	seelogConfig := fmt.Sprintf(`
	<seelog minlevel="info">
		<outputs formatid="main">
			<console/>
			<rollingfile type="size" filename="logs/replica/replica_%s.log" maxsize="1000000" maxrolls="5"/>
		</outputs>
		<formats>
			<format id="main" format="%%Date %%Time [%%LEVEL] %%Msg%%n"/>
		</formats>
	</seelog>
	`, port)

	configBytes := []byte(seelogConfig)

	return seelog.LoggerFromConfigAsBytes(configBytes)
}