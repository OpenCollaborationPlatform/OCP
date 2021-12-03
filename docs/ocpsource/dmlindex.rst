 
Datastructure Markup Language
=============================
 
With dml you define your datastructure and its inherent logic in a simple, hirarical and integrated way. It is intended to 
be be easy to write and read, and to make the interaction from your application straigthforward. The main drivers for the design 
are:

1. Be very flexible in creating structured datastorage that fits the diverse needs of applications
2. Work well with the WAMP RPC and Pub/Sub systems
3. Allow to integrate Logic into the data in a natural way
 
To achieve this DML enables the user to create composite datastructures from common types like maps and list (driver 1), while providing 
functions and events for those base and composite types, that directly map to the WAMP RPCs and Pub/Sub system (driver 2). Finally the direct
integration of JavaScript into the datatypes that can be used for events and functions gives you plenty of possibilities to define custom 
logic (driver 3). All this can be seen in the following example:

.. code-block:: javascript
    
   Data {
        .name: "MyData"
        
        Map {
            .name:  "MyMap"
            .key:   string
            .value: int
        }
        
        Vector {
            .name: "MyVector"
            .type: string
        }
        
        event onAdding
        
        function Add(number, text) {
        
            this.MyMap.Set(text, number)
            this.MyVector.Append(number)
            this.onAdding.emit()
        }
    }
 
.. toctree::
    language
    gen_dmltypes
    gen_systems
    gen_dml

    
