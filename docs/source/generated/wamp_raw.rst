.. wamp:procedure:: ocp.documents.<docid>.raw.CidByBinary(uri, arguments)

	Adds raw binary data to the document. To support unlimited size data the binary stream is broken up in packages
	and a progressive WAMP procedure is used to deliver the packages. However, as only progressive read is supported,
	and not write, the user needs to provide a progressive wamp procedure the node can call. The arguments to
	CidByBinary are the URI of this procedure as well as any argument you want it to receive. The node than calls
	your procedure with those arguments and collects the binary data.

	:path DocID docid: ID of the document the data shall be added to
	:param Uri uri: The uri of the progressive WAMP procedure that delivers the binary data stream
	:param any arguments: All arguments that shall be provided to the progressive procedure to identify the correct data
	:result cid: Content identifier for the binary datastream
	:resulttype cid: Cid


.. wamp:procedure:: ocp.documents.<docid>.raw.BinaryByCid(uri, arguments)

	Reads raw binary data from the document. To support unlimited size data the binary stream is broken up in packages,
	hence this is a progressive WAMP procedure. You need to collect all datapackages sent and combine them into the
	final binary data.

	:path DocID docid: ID of the document the data shall be added to
	:param Uri uri: The uri of the progressive WAMP procedure that delivers the binary data stream
	:param any arguments: All arguments that shall be provided to the progressive procedure to identify the correct data
	:result Cid cid: Content identifier for the binary datastream


.. wamp:procedure:: ocp.documents.<docid>.raw.CidByPath(path)

	Reads raw data from the filesystem. It adds all the content in path to the document,
	including all subdirectories recursively. The returned cid will be valid for the
	full structure, all files and directories, and it is not possible to get a cid for
	individual files or subdirs. The structure stays intact, and when extracting the data
	again into a filesystem will be reproduced.

	.. note:: The content in path is copied without any restrictions or filtering

	:path DocID docid: ID of the document the data in path shall be added to
	:param str path: Absolute filesystem path to file or directory
	:result Cid cid: Content identifier for the data in path


.. wamp:procedure:: ocp.documents.<docid>.raw.PathByCid(cid, path)

	Write data stored in the document into the given path. If the cid describes a binary stream or file,
	a file will be created, if it is a directory it will be recreated with the original structure.
	The name of files or toplevel directories are not stored and hence not recreated. The name will
	be the Cid. Use the return value to get the newly created path with full name.

	:path DocID docid: ID of the document the data in path shall be added to
	:param Cid cid: The content you want to store in the filesystem
	:param str path: Absolute filesystem to directory to create the content in
	:result str path: Path of the newly created file or directory


