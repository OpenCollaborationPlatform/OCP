Peer To Peer
------------
Currently only monitoring the p2p status is possible, not maipulating it.


.. wamp:procedure:: ocp.p2p.id()

	Get the id of the node

	:return NodeID id: The id of the node


.. wamp:procedure:: ocp.p2p.adresses(shortened)

	Get all the adresses the node is reachable with. This includes only adresses, the node
	knows about Hence it can change over lifetime, if we learn about new ones, for example after connecting
	successfully to a public node, or after connecting locally to annother node.

	:param bool shortened: The listed adresses do not include the NodeID if True
	:return list[str] adresses: List of p2p IP adresses the node is reachable with


.. wamp:procedure:: ocp.p2p.peers()

	Return all peers we are currently connected to

	:return list[NodeID] peers: The list of all connected peers


.. wamp:procedure:: ocp.p2p.reachability()

	Returns the reachability status. This indicates, if we are reachable by other nodes
	from outside (reachabilit = public) or if they cannot connect to us (reachability = private)
	Initially the eachability is "unknown", and only after the node connected to a bootstrap node
	it can be checked and updated.

	:return str reachability: "Public", "Private" or "Unknown"



.. wamp:event:: ocp.p2p.ocp.p2p.peerConnected

	Emitted when a peer connected to this node.

	:arg NodeID peer: The peer that connected

.. wamp:event:: ocp.p2p.peerDisconnected

	Emitted when a peer disconnected from this node.

	:arg NodeID peer: The peer that disconnected

.. wamp:event:: ocp.p2p.addressesChanged

	Emitted when the known adresses of the node change.

	:arg list[str] addrs: All known p2p adresses of the node

.. wamp:event:: ocp.p2p.reachabilityChanged

	Emited when the reacahbility status changed. It is initially always unknown,
	and any update to it will be published.

	:arg str reachability: New reachability status, "Public", "Private" or "Unknown"


