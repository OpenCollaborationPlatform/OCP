
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


