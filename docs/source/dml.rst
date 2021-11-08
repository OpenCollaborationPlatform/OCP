
DML api
=======



.. dml:object:: Map

	Mapping from any key type to any other type. A Map is a standart datatype as
	avilable in all programming languages, sometimes nown as Dictionary.

	.. dml:property:: key
		:const:
		:type: key
		:default: string

		A property defining the datatype of the maps keys. Allowed values are all
		key datatypes like int and string.

	.. dml:property:: value
		:const:
		:type: type
		:default: none

		A property defining the datatype of the maps values. Allowed values are
		dml types including var. This allows for nesting objects by making the map
		value a new subtype.


	
	.. dml:function:: Length()
	
		Returns the length of the map,  which is defined as the number of keys
	
		:return int l: The length of the map
	
	

