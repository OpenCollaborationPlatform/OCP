package connection

func MakeTemporaryRouter() (*Router, error) {
	
	r := NewRouter(nil)
	r.Start(make(chan string,0))
	return r, nil
}