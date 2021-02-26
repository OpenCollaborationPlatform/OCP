package connection

import (
	"bytes"
	"io/ioutil"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

func MakeTemporaryRouter() (*Router, error) {

	//a logger to discard all logs (to not disturb print output)
	testLogger := hclog.New(&hclog.LoggerOptions{
		Output: ioutil.Discard,
	})

	r := NewRouter(testLogger)
	r.Start(make(chan string, 0))
	return r, nil
}

func MakeTwoTemporaryRouters() (*Router, *Router, error) {

	// we need to make sure that all routers use different ports!
	jsonConf := []byte(`
		{
	    		"connection": {
		    		port: 1000
				}
		}`)

	viper.ReadConfig(bytes.NewBuffer(jsonConf))
	r1, err := MakeTemporaryRouter()
	if err != nil {
		return nil, nil, err
	}

	jsonConf = []byte(`
		{
	    		"connection": {
		    		port: 1001
				}
		}`)

	viper.ReadConfig(bytes.NewBuffer(jsonConf))
	r2, err := MakeTemporaryRouter()
	if err != nil {
		return nil, nil, err
	}

	return r1, r2, nil
}
