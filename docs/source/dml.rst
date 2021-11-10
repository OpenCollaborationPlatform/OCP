
API
---


.. dml:object:: Data

	The most basic implementation of a DML Object. It allows to add properties, events
	and functions and can hold other Objects and Behaviours as children. It has no other
	special functionality. It is intended as dml grouping 	object as well as base object
	for all other data types
	Data does allow for children. Note that children are static values, they cannot change at
	runtime. Hence they are different to dynamic objects as are possible with Maps etc. Children are
	used purely for the static DML hirarchy.

	.. dml:property:: name
		:const:
		:type: string

		A property defining the name of the object. The name can than be used to access in
		the hirarchy, either in JavaScript code or as WAMP uri. It is mandatory to set the name
		of each object.

	.. dml:event:: onBeforePropertyChange

		Emitted bevore a property of the object changes. At time of emit the
		property still has its old value.

		:argument string Property: Name of the property thats about to be changed

	.. dml:event:: onPropertyChanged

		Emitted when a property was changed. The value of the property is already the
		new one when emitted.

		:argument string Property: Name of the property thats was changed

	.. dml:event:: onCreated

		Emitted after the object was created and fully setup. It will be emitted
		only dynamically created objects, for example when used as value in Maps,
		and not the ones in the static DML hirarchy.

	.. dml:event:: onRemove

		Emitted after the object is about to be removed. At the time of emitting
		the object is still fully setup and accessible, the removing will happen after
		all handlers have been executed.
		As static hirarchy objects cannot be removed this event will be emitted only
		for dynamically created objects, for example when used as value in Maps etc.

	.. dml:event:: onBeforeChange

		This is a general event, emitted bevore the object itself changes, no matter
		what the changes are. This is not emitted for changed properties, there is a
		custom event for that, but if the objects content is manipulated. This means that
		for a Data object it will never be emitted, as it does not have any object
		content, but it may be emitted for derived object types like maps.

		.. note:: Most derived classes that emit this event will also have custom
				  events that are more specialized for the eexact changes that happend.

	.. dml:event:: onChanged

		This is a general event, emitted after the object has changed, no matter
		what the changes are. This is not emitted for changed properties, there is a
		custom event for that, but if the objects content was manipulated. This means that
		for a Data object it will never be emitted, as it does not have any object
		content, but it may be emitted for derived object types like maps.

		.. note:: Most derived classes that emit this event will also have custom
				  events that are more specialized for the eexact changes that happend.



.. dml:object:: Map
	:derived: Data

	Mapping from any key type to any other type. A Map is a standart datatype as
	avilable in all programming languages, sometimes nown as Dictionary.

	.. dml:property:: key
		:const:
		:type: key

		A property defining the datatype of the maps keys. Allowed values are all
		key datatypes like int and string.

		:default: string

	.. dml:property:: value
		:const:
		:type: type

		A property defining the datatype of the maps values. Allowed values are
		dml types including var. This allows for nesting objects by making the map
		value a new subtype.

		:default: none


	
	.. dml:function:: Length()
	
		Returns the length of the map,  which is defined as the number of keys
	
		:return int l: The length of the map
	
	

.. dml:behaviour:: Behaviour
	:abstract:

	Base class for all behaviours, adding common properties and events. It cannot be
	used directly, only behaviours derived from it. It does add the possibility to add
	custom properties,  events and functions. Children are not allowed.

	.. dml:property:: name
		:const:
		:type: string

		A property defining the name of the behaviour. The name can be used to access ut in
		the hirarchy, either in JavaScript code or as WAMP uri. It is mandatory to set the name
		of each behaviour.

	.. dml:property:: recursive
		:const:
		:type: bool

		Defines if the behaviour is applied recursively for all children and subobjects
		of the behaviours parent. For example, if a behaviour is added to a Map Object,
		it may watch for changes in that object. If recursive is true, it will also look
		for all changes in any children or value objects of that Map.

		:default: false

	.. dml:event:: onBeforePropertyChange

		Emitted bevore a property of the object changes. At time of emit the
		property still has its old value.

		:argument string Property: Name of the property thats about to be changed

	.. dml:event:: onPropertyChanged

		Emitted when a property was changed. The value of the property is already the
		new one when emitted.

		:argument string Property: Name of the property thats was changed


