Documents
---------


.. wamp:procedure:: ocp.documents.create(path)

	Creates a new document from the provided dml code. The code must be in a
	directory called "Dml", to which the path parameter points. Inside the drectory
	the main file must be called main.dml. Apart from that arbitrary named
	files are allowed to be in the folder and can be used in import statements from
	main.dml

    :param str path: Path to folder called dml which contains the docuemnt code
    :return DocID docid: The ID of the newly created document


.. wamp:procedure:: ocp.documents.open(docid)

	Opens the document with the given id. For this the node looks for peers that
	have it open already and connects to them. If no peers are found the opening
	fails. It then asks to join the document, which is accepted or denied by the
	other peers depending on your nodes authorisation status in the document. It
	hence is important to make sure your node is allowed to join.

	.. note:: Only nodes that are already part of the document and have write
			  permissions are allowed to add your node into the document configuration.
			  One of these nodes needs to do so before "open" call will be successfull.

    :param DocID docid: The id of the document which should be opened


.. wamp:procedure:: ocp.documents.close(docid)

	Closes the document on the node and informs all other peers that we are leaving.
	Fails if the document is not open on the  node.

    :param DocID docid: The id of the document which should be closed


.. wamp:procedure:: ocp.documents.list()

	Lists all currently open documents on the node.

    :return list[DocID] docs: All documents that are open


.. wamp:procedure:: ocp.documents.invitations()

	Lists all invitations, hence all documents we know of where we are allowed to
	join, but did not open it yet.

    :return list[DocID] docs: All documents we are invited in


.. wamp:procedure:: ocp.documents.updateInvitations()

	Searches the network for documents we are allowed to join, and republish
	a invitation event for each one found. Before the search however all known
	invitations are canceled by publishing an "invited" event with the uninvite arguments.


.. wamp:event:: ocp.documents.created

	Emitted when a new document was created on the node. This event is not received by
	the client calling the ocp.documents.create procedure, but by all other clients
	connected to the node.

	:argument DocID id: The ID of the document that was created on the node

.. wamp:event:: ocp.documents.opened

	Emitted when a document was opened on the node. This event is not received by
	the client calling the ocp.documents.open procedure, but by all other clients
	connected to the node.

	:argument DocID id: The ID of the document that was opened on the node

.. wamp:event:: ocp.documents.closed

	Emitted when a document was closed on the node. This event is not received by
	the client calling the ocp.documents.close procedure, but by all other clients
	connected to the node.

	:argument DocID id: The ID of the document that was closed on the node

.. wamp:event:: ocp.documents.invited

	Emitted when our invitation status in any document changed. This happens if
	annother node adds our node to a documents configuration (invtation=True) or
	if we are removed from 	it (invitation=False). Once we received this invent
	with invitation=True we can call ocp.documents.open for this document.

	:argument DocID id: The ID of the document in which our invitation status changed
	:argument bool invitation: True if we were invited, False if uninvited



Handling individual documents
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Individual documents are accessed with their document ID simply by using the wamp
procedure ocp.documents.*docid* . From there you have the change to manage the
documents peers by adding, removing or changing their rigths. But you can also
access all the content in that document, be it the RAW data you stored in it or
the structured data according to your dml file.


.. wamp:uri:: ocp.documents.<docid>.content.<dmlpath>

	Access anything within the document that is defined by the DML code. As the
	dml code builds up a hirarchical datastructure where each object has its own
	name, it can be easily be transformed into a WAMP uri. This is then used to
	access individual objects, properties or events. The end result of where the URI
	points to defines than the behaviour and how it should be used from the client:

	* Object: Calling the procedure returns the ObjID
	* Property: Calling the procedure without arguments returns the value, with an argument sets the property value
	* Event: Subscribing to the topic receives all event emits
	* Function: Calling the procedure calls the function and returns its results

	:path DocID docid: The document whichs content should be accessed
	:path str dmlpath: Mutli state procedure to the dml object


.. wamp:procedure:: ocp.documents.<docid>.execute(code)

	Execute the provided javascript code in the document.

	:path DocID docid: The document in which the code shall be executed
	:param str code: JavaScript code that will be executed within the document
	:return: Returns the outcome of the code


.. wamp:procedure:: ocp.documents.<docid>.addPeer(peer,auth)

    Adds new peer to the document

    :path DocID docid: ID of the document to add the peer to
    :param NodeID peer: Peer to add to the document
    :param str auth: Authorisation of added peer


.. wamp:procedure:: ocp.documents.<docid>.removePeer(peer)

	Remove the peer from document

    :path DocID docid: ID of the document to add the peer to
    :param NodeID peer: Peer thats autorisation should change


.. wamp:procedure:: ocp.documents.<docid>.listPeers(auth="None", joined=False)

	List all peers in the document, possibly filtered auth or joined peers

    :path DocID docid: ID of the document to add the peer to


.. wamp:procedure:: ocp.documents.<docid>.setPeerAuth(peer,auth)

	Change the peer authorisation

    :path DocID docid: ID of the document to add the peer to
    :param NodeID peer: Peer thats autorisation should change
    :param str auth: New authorisation


.. wamp:procedure:: ocp.documents.<docid>.getPeerAuth(peer)

	Read the peer authorisation

    :path DocID docid: ID of the document to add the peer to
    :param NodeID peer: Peer thats autorisation should change
    :param str auth: New authorisation


.. wamp:procedure:: ocp.documents.<docid>.hasMajority()

	List all peers in the document, possibly filtered auth or joined peers

    :path DocID docid: ID of the document to add the peer to
    :return bool majority: True or false, dependent if majority is available


.. wamp:procedure:: ocp.documents.<docid>.view(open)

	List all peers in the document, possibly filtered auth or joined peers

    :path DocID docid: ID of the document to add the peer to
    :return bool open: True or false, dependent if majority is available


.. wamp:event:: ocp.documents.<docid>.peerAdded(peer)

.. wamp:event:: ocp.documents.<docid>.peerRemoved(peer)

.. wamp:event:: ocp.documents.<docid>.peerAuthChanged(peer)

.. wamp:event:: ocp.documents.<docid>.peerActivityChanged(peer)



Raw data handling
^^^^^^^^^^^^^^^^^
OCP documents support raw binary data as mass storage for complex and custom
data. It can be added directly from the filesystem or as binary datastream with
the provided procedures. If used the caller is responsible for storing the
content identifiers in the document, it is not done automatically.


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


