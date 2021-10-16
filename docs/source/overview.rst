Overview
========

Traditional most applications are build with a datamodel suitable for single user access, all data update algorithms and storge policies are build with the mindset of one application manipulating the data. In this scenario the app is responsible for ensuring data integrity and validity, and it usually does so by guiding the user with allowed or forbidden interactions. This works well as user input comes in syncronously, one action after annother, and hence the application can ensure that only valid actions can occure based on current state of the data.

However, if the same application wants to enable real time multi user collaboration on the same data, this very basic principle of syncronous input is not valid anymore. It could easily happen that two users on different machines do different actions at the same time. Then the question arises which applications state is valid, and what should be the common base for the next allowed action?


Shared asyncrounous data structure
----------------------------------

Our solution for this problem is the creation of a shared asyncronous data structure parallel to the applications internal one. 
