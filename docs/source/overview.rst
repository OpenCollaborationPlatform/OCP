Overview
========

Traditional most applications are build with a datamodel suitable for single user access, all data update algorithms and storge policies are build with the mindset of one application manipulating the data.

|tradition_application|

In this scenario the app is responsible for ensuring data integrity and validity, and it usually does so by guiding the user with allowed or forbidden interactions. This works well as user input comes in syncronously, one action after annother, and hence the application can ensure that only valid actions can occure based on current state of the data.

Now assume you want to enable the given application for direct, realtime collaborative editing. The first thing that comes to mind is to simply forward all user inputs from one application to the other. 

|collaboration_application|

However, it becomes imediately clear that the principle of syncronous input is not valid anymore: there are multiple input sources now for the application. Even worse, the remote input source is delayed and the user actions can arrive unordered. This means that the external inputs may not fit the current application state, and hence need to be rejected. This information must of course be provided back to the source, but this takes time again, and it could easily be that the source applicaton is already way ahead of the rejected action. This approach is clearly flawed, and it is almost impossible to keep the applicaion states syncronized this way.

It becomes clear that there must be a central entity which defines the current state for all applications, some kind of system that every update is written to and where each application can inquery the currently valid data. Using a database for that comes to mind. It does provide a consistent view on data and allows to be accessed from multiple applications worldwide. Pefect, rigth? Not really. Imagine the following sequence of events:

|database_problem|

This would result in both applications think the sate is different. Of course the applications can read the state again to make sure they have it rigth, but it still would not have any guarantee that the state is still valid when the query finished. But when a application cannot be sure what the current state is, how can it write a new one, as the update may not be allowed?

.. note:: Databases do not provide any way of informing about updates. So you need to inform the other aplications about your changes manually, which provides almost the same challenge of synchronisation and ordering as the state manipulation itself

We can conclude that some verification logic is needed next to the database, that makes sure only valid writes occure and makes updates fail if invalid.

.. |tradition_application| image:: ../images/traditional_application.png
.. |collaboration_application| image:: ../images/collaborative_application_problem.png
.. |database_problem| image:: ../images/database_problem.png


Shared custom data and logic
----------------------------

Our solution for this problem is a framework that lets you easily create custom datastructures, including all kinds of custom logic. You create the structure and logic needed for your application, OCP handles the syncronisation and data transfer across unlimited clients. They main building blocks you use are:

* **Document**: A document is the container for all your custom types and logic. A document is basically your created database
* **Data objects**: Objects are the main building block for a hirarchical data structure withi the document. There are predefined ones like Map and List, and from those you build your own objects by combining and cutomizing them.
* **Properties**: Each provided object has properties that define its behaviour. You can also add your own, eihter for cutomisation of your code or for data storage
* **Functions**: You code custom functions within your objects, which are callable from other objects and directly from your application
* **Events**: Each provided object has a set of predefined events, like "onNewEntry" for a map, and you can even define your own events. You can define code within the objects that will be executed on event emitting, or listen for the events in your application 

See for example the following data structure:

|custom_datastructure|

This document provides you with a list that holds custom maps. You can add additional of those custom maps by calling MyMainObject.AppendNew(), remove entries, access them for manipulation etc.. Each entry in this list is a Map object, in our case with string keys and integer values. Of course they could be other types, and the values even annother be custom objects, further building your hirarchy. With this you are extremely flexible building a data layout to your needs.

Now to the logic: The main object has a function which is called when event D is emitted. This could for example be the "onNewEntry" event of the list. The function is than called, and could check if it is ok to create a new entry based on all available data. If yes, fine, if not it can raise an error and the whole update fails. See here for comparison the timelines for a successfull and a failing update of the shown document:

|document_update|

With this tool at your hand you are able to create a datastructure that is shared by all applications that want to edit a given document. When the user does a editing action, you write it to your internal datastructure as usual, no changes there. Additional you than try to replicate this change on the shared document. If successfull, you go on. If failed, you can inform the user and restore the initial state (which you can read from the document)

.. |custom_datastructure| image:: ../images/custom_data_structure.png
.. |document_update| image:: ../images/document_update.png



Datastructure Markup Language
-----------------------------



Peer to Peer setup
------------------
