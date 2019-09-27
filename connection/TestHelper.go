package connection

import (
	"bytes"
	"github.com/spf13/viper"
)

func MakeTemporaryRouter() (*Router, error) {
	
	r := NewRouter(nil)
	r.Start(make(chan string,0))
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