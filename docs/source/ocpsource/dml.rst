
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
	
		Returns the length of the map,  which is defined as the number of keys.
	
		:return int length: The length of the map
	
	

	
	.. dml:function:: Keys()
	
		Provides access to all keys in the map. The type of the keys is dependend on
		the Maps *key* property. If called from javascript, an unmutable array is returned,
		if called via WAMP API it will be the list type that is supported by the
		calling language  (e.g. List for python)
	
		:return List[any] keys: All keys that are in the map
	
	

	
	.. dml:function:: Has(key)
	
		Checks if the given key is available in the Map. The key must be of correct
		type, e.g. if the Map key property defines int, the key must be an integer and
		not a string describing the integer (like 1 and not "1"). This is different to
		how the Map handels WAMP Uri for accessing its values, as there the key must always
		be given as string.
	
		:throws: If key is of wrong type
		:return bool has: True if the key is available in the Map
	
	

	
	.. dml:function:: Get(key)
	
		Returns the value currently stored for the given key. If the Maps value type
		is a Object it will return the object itself when called from JavaScript, and
		the object ID if called by WAMP Api.
	
		:throws: If key is not available
		:throws: If key is of wrong type (must be equal to key property)
		:return any value: The value for the key
	
	

	
	.. dml:function:: Set(key, value)
	
		Sets the value for the given key. If already available it the old value will
		be overriden, otherwise it will be newly created and set to value. Key and value type
		need to be consitent with the Maps defining properties.
	
		If the Map value type is a object, this function will fail. It is not possible,
		to set it to a different object or override it once created. Use the *New* function
		for creating the object for a given key.
	
		:throws: If key is of wrong type (must be equal to key property)
		:throws: If value is of wrong type (must be equal to value property)
		:throws: If value type of Map is a Object
	
	

	
	.. dml:function:: Net(key)
	
		Creates a new entry with the given key and sets it to the value types default,
		e.g. 0 for int, "" for string etc. If the value type is a Object it will be fully
		setup, and its onCreated event will be called. The *New* function is the only way to
		create a key entry if value type is an Object, as *Set* will fail in this case.
	
		:throws: If key already exists
		:throws: If key is of wrong type (must be equal to key property)
		:return any value: The stored value for given key
	
	

	
	.. dml:function:: Remove(key)
	
		Removes the key from the map. If value type is a Object, its onRemove event
		will be called and afterards will  be deleted.
	
		:throws: If key does not exist
		:throws: If key is of wrong type (must be equal to key property)
	
	

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


