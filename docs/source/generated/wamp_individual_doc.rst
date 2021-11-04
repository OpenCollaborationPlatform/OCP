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


