// FileHandler
package main

type FileSharing struct {
}

func (fs *FileSharing) start() {

	/*
		//setup the ipfs node
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		path, _ := config.PathRoot()
		if !fsrepo.IsInitialized(path) {

			conf, err := config.Init(os.Stdout, 2048)
			if err != nil {
				log.Error("Config failed")
				return
			}
			if err := fsrepo.Init(path, conf); err != nil {
				log.Error("Repo Init failed")
				return
			}
		}

		r, err := fsrepo.Open(path)
		if err != nil {
			// Deal with the error
		}
		cfg := &core.BuildCfg{
			Repo:   r,
			Online: true,
		}

		node, err := core.NewNode(ctx, cfg)
		if err != nil {
			// Deal with the error
		}

		defer func() {
			// We wait for the node to close first, as the node has children
			// that it will wait for before closing, such as the API server.
			node.Close()

			select {
			case <-ctx.Done():
				log.Warning("Gracefully shut down daemon")
			default:
			}
		}()

		log.Debug(node.Identity.Pretty())

		time.Sleep(5 * time.Second)*/
}
