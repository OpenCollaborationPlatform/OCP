User
----
User handling is almost non-existent at the moment. Their is no user index,
identification or data handling. All there is at the moment is a way to set a
username for a node to make it easier findable for humans.


.. wamp:procedure:: ocp.users.set(name)

	Sets the user name for this node. User names are just hints at the moment,
	any node can use any name, and it is not checked for duplicates. This
	is only to make searching a node id possible.

	:param str name: Name for the given node


.. wamp:procedure:: ocp.users.find(name)

	Searches the network for a node with the given user name. The name must be
	exact, including cases, there is no matching done.

	:param str name: Name to search node for
	:return: NodeID of the first found node with the given username


