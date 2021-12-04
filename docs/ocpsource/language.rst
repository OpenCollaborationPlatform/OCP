Language
========

DML is a multi-paradigm language language for creating shared datastructures. With DML, datastructure building blocks are declared and various properties set 
to define their behavior. Datastructure behavior can be further scripted through JavaScript, which is a subset of the language. This reference guide describes
the features of the DML language. Note that we took heavy inspiration from Qt's QML language.

.. note:: DML is not a professionally designed language, but created on the fly as needed. You will find many
          quirks and inconsistencies. If you have knowledge about languae design and compilers please consider 
          getting involved into the project.

Syntax Basics
-------------

DML enables data objects to be defined by common types and through their attributes, and how they interact with certain general systems through custom object
behaviours. DML source code is generally loaded by the engine through DML documents, which are standalone documents of DML code. These can be used to define
DML object types that can then be reused throughout a datastructure.

Object Declarations
+++++++++++++++++++

Syntactically, a block of DML code defines a tree of DML objects to be created. Objects are defined using object declarations that describe the type of object 
to be created as well as the attributes that are to be given to the object. Each object may also declare child objects using nested object declarations.

An object declaration consists of the name of its object type, followed by a set of curly braces. All attributes and child objects are then declared within 
these braces.

Here is a simple object declaration:

.. code-block:: JavaScript

    Data {
        .name: "MyObject"
    }

This declares an object of type Data, followed by a set of curly braces that encompasses the attributes defined for that object. The :dml:obj:`Data` type
is a type made available by DML itself, and the attribute which is assigned in this case is the objecs name. This is a property available for all object types.

There are two different object types in DML: dataobjects and behaviours. While dataobjects are used to store data and create the structure you need, 
behaviour objects specialize dataobjects and how they react in special circumstances or interact with DML systems.

Child Objects
+++++++++++++

All dataobjects declarations (but not behaviours) can define child objects through nested object declarations. In this way, any dataobject declaration 
implicitly declares an object tree that may contain any number of child objects. For example, the *Data* dataobject declaration below includes a *Map* 
dataobject declaration.

.. code-block:: JavaScript

    Data {
        .name: "MyObject"
        
        Map {
            .name: "MyMap"
            .key: int
            .value: string
        }
    }

The list of children can be accessed with the :dml:prop:`Object.children` property, while a objects parent can be accessed with the 
:dml:prop:`Object.parent` property.

Any object can be a child of a dataobject, also behaviours, and this is the way to define behaviours for a certain object. For 
example, enabling the base :dml:obj:`Data` type for transactions can be done by adding a Transaction behaviour

.. code-block:: JavaScript

    Data {
        .name: "MyObject"
        
        Transaction {
            .name: "transaction"
            .automatic: true
        }
    }

The Transaction behaviour itself cannot have any children.

Imports
+++++++

Declared DML objects are instanciated by the engine. However, sometimes one does not directly want a object to be instanciated but use it as
a custom type to be further used in other object declarations. This can be done with imports. If a dml file with a given name is imported, a
new type is created with the filename (without extension) as typename. If annother typename is desired an alias can be given with the optional
`as` keyword.

.. code-block::

    import <filename> [as <Qualifier>]

This allows to reuse custom object declarations. Nte that it is possible to override property default values for imported objects the same
way as is possible for build in types.  Assuming this is defined in "myobject.dml":
   
.. code-block:: JavaScript

    Data {
        .name: "MyObject"
        
        property int myproperty: 1
    }
    
it can be used and customized as follows by annother dml file:

.. code-block:: JavaScript

    import "myobject.dml" as MyObject

    Data {
        .name: "MyObject"
        
        MyObject  {
            .name: "MyObject1"
            .myproperty: 2
        }
        MyObject  {
            .name: "MyObject2"
            .myproperty: 3
        }
    }

Comments
++++++++

The syntax for commenting in DML is similar to that of JavaScript:

* Single line comments start with `//` and finish at the end of the line.
* Multiline comments start with `/*` and finish with `*/`

.. code-block:: JavaScript

    Data {
        .name: "MyObject"
        // this is my single line comment
        
        /*  Good thing this assignment is ignored, 
            as this is not a property of Data
        .imaginary_property: 12
        */
    }
    
    

DML Object attributes
---------------------

An object declaration in a DML document defines a new type. Each instance of an object type is created with the set of attributes that have
been defined for that object type. There are several different kinds of attributes which can be specified, which are described below.

Property
++++++++

A property is an attribute of an object that can be assigned a static value. A property's value can be read by other objects or by the user via WAMP API.
Generally it can also be modified by another object or the users, unless a particular QML type has explicitly disallowed this for a specific property.

A custom property of an object type may be defined in an object declaration in a DML document with the following syntax:

.. code-block::

    [const] property <propertyType> <propertyName> [: <defaultValue>]

In this way an object declaration may expose a particular value to outside objects or maintain some internal state more easily. Property names must begin
with a letter and can only contain letters, numbers and underscores. JavaScript reserved words are not valid property names. The const keyword is optional.

Allowed propery types are all DML :ref:`Datatypes`. Therefore it is possible to use strict typing or *var* to hold anything the user wants.

.. code-block:: JavaScript

    property var myNumber: 3.1
    property var myString: "abc"
    property var myBool: true
    property var MyObject: Data{}    

..  note:: *var* allows also data that is not expressible in DML code but can be written in JavaScript or via WAMP, like arrays and maps.

The optionally provided value is considered the default value for the property, and used as long as no user changes it. If no default value is provided
the standart default value for the type is used, e.g. 0 for `int`.  

.. code-block:: JavaScript

    property int myNumber: 1    // default value = 1
    property int yourNumber     // default value = 0
    
Constant means the property is not changable at runtime. It is however changable at creation time, and the user can e.g. override the default value
of a build in const property of some Object in the dml code. Also, if a user reuses custom objects via `import`, he is able to set a new default
value of his self created const properties. This makes const properties useful for configuration properties, detailing the objects functionality.

Method
++++++

Methods can be added to a DML type in order to define standalone, reusable blocks of JavaScript code. These methods can be invoked either by other
objects or via the WAMP api.
  
.. code-block::
    
    [const] function <methodName> ([<parameterName>[, ...]]) { <JavaScript> }
    
Method parameter types do not have to be declared as they default to the var type. Attempting to declare two methods with the same name in the same 
object type block is an error, but the first declaration is ommited. For build in types declarations with the same name are not considered, except
for some special methods are are intendet to be overridden by the user. Those are markt *virtual* in the documentation.

Within a method *this* is the object the method is defined in, and can hence be used to access proeprties, other methods or the hirarchy.

.. code-block:: JavaScript

    Data {
        .name: "MethodTest"
        
        property int myProp: 1
        
        function myMethod( newValue ) {
            this.myProp = newValue
        }
    }


Declaring a method as constant means that it does not change any data, only reads it. This has no impact for JavaScript calling of the method, but 
allows to heavily optimize and speed up the function execution when called by the user vie WAMP api. When such a const function is called no data 
alignment between all users is required, and hence local data storage can be used to execute the function, which is much faster. There is no compile
time check if a const method does really not change any data, or call any non-const function, but after returning a WAMP call to const method any 
changes to the data are reverted automatically, to enforce the const. There will be no user feedback for this, so work carefully.

Event
+++++

Events can be added to a DML objects to define action based callback handling. Events can either trigger JavaScript callbacks assigned to them, or
catched and processed via the WAMP api. 

.. code-block::
    
    event <eventName> [: function <methodName> ([<parameterName>[, ...]]) { <JavaScript> }]
    
Note that a event does not define its arguments, and any emit (or WAMP publish) can add in any arguments it wants. However, if a callback does not support 
the amount or type of arguments a error occurs. It is therefore important for the user to ensure all emits and callbacks are defined with same arguments in mind.

A default callback can be assigned to the event during the declaration by adding a function.  Alternatively a callback can be added (and removed) from JS code 
during runtime by registering it in the event. This works via the object a callback method is defined in and the callback name.

.. code-block:: JavaScript
    
    Data {
        property int myProp: 0
        
        event myEvent: function {
            this.myProp = this.myProp + 1
        }
        
        function myCallback() {
            this.myProp = this.myProp + 1
        }
        
        function registerAndEmit() {
            //add callback in this object, but any other would work too
            this.myEvent.RegisterCallback(this, "testEventCallback")
            
            //emit the event
            this.myEvent.emit()
            
            //both callback are called
            //myProp == 2
        }
    }
    
For a default callback, *this* is the object the event is defined in. For registered callbacks *this* is the object the method is defined in. 
Emitting events works by calling *emit* on it via JavaScript, or via publishing a message via wamp. 

.. note:: Event callbacks in JS are all process directly with a call to *emit*, in order of registration, and the JS code only continues after
          all callbacks are finished. It works the same for predefined object events. This does however not hold for WAMP api events: those are
          published after all JS processing is done, and only if no error occured during the processing.

Document Structure
------------------

The dml code is added to files with the extension `.dml` and needs to be put into a folder named `dml`. Each `*.dml` file can contain a single toplevel Object, as well
as multiple import statements to use types defined in other files. The datastructures toplevel Object is the one defined in the file named `main.dml`. This file is 
mandatory, and an error occurs if not available.
