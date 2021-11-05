WAMP API
========
test ref :wamp:proc:`open` test ref

Index: :ref:`wamp-index`


* :ref:`wamp-index`

Documents
---------

.. include::  generated/wamp_doc.rst

Handling individual documents
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Individual documents are accessed with their document ID simply by using the wamp
procedure ocp.documents.*docid* . From there you have the change to manage the 
documents peers by adding, removing or changing their rigths. But you can also 
access all the content in that document, be it the RAW data you stored in it or 
the structured data according to your dml file.

.. include::  generated/wamp_individual_doc.rst

Raw data handling
^^^^^^^^^^^^^^^^^
OCP documents support raw binary data as mass storage for complex and custom
data. It can be added directly from the filesystem or as binary datastream with the provided procedures. If used the caller is responsible for storing the content identifiers in the document, it is not done automatically.

.. include::  generated/wamp_raw.rst

