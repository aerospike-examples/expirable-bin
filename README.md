Expirable-Bin
=============

Provide bin-level expiration functionality in Aerospike, using User Defined Functions.

For a front-edge, operational database, expiration of data is a key feature. Aerospike
supports a time-to-live on every record (row) in the database, and automated expiration
of those records - both foreground expiration, and background "eviction". This allows
running the database in a "run full" cache-like configuration.

However, an important case is sub-record expiration. This library shows how to provide
expiration on an individual cell basis. As the library uses Aerospike's UDF functionality,
it can be modified to provide other policies, or sub-cell expiration.

The approach is to provide User Defined Functions to access (read and write) bin (column) values.
By mediating all reads and writes through the UDFs, extra data such as the expiration time
can be checked, stored, and updated.

The scan function provides an off-the-shelf routine to reclaim space. Execute this batch
job occasionally to reclaim space, through Aerospike's UDF Scan functionality.

The module provides:  
**put** - Insert bins with optional time-to-live in seconds, -1 for no expiration.   
**get** - Return bins that are not expired.  
**touch** - Update the bin time-to-live.  
**ttl** - Return bin time-to-live in seconds.    
**clear** - Scan the database, and clear out expired bins.  

Client interface is available for Java, C, Python, and Lua

#Installation

The source for this module is available on Github, at https://github.com/aerospike/expirable-bin.git
Clone the Github repository using the following command:
```
git clone https://github.com/aerospike/expirable-bin.git
```

Register the lua file using aql,
```aql
aql -c "register module 'expire_bin.lua'"
```

#Use

This module can be used from client calls or within other UDFs. For examples of client
calls, see the C and Java examples under ```src/c``` and ```src/java```. Please ensure
that the expire_bin.lua file is registered to the server before running the examples.

###C 
The C code depends on the Aerospike C library, you can either download the demo package from the Aerospike [website](http://www.aerospike.com/docs/client/c/) or you can go to [GitHub](https://github.com/aerospike/aerospike-client-c) to install and follow the instructions.

To run the C example: 
```
make run 
```
For simplicity, the Makefile assumes Lua is the default one that is included in ```aerospike.a``` library, if you want to have a different kind of Lua included please go see Aerospike [C Client](https://docs.aerospike.com/display/V3/C+Client+Guide).

###Java
The Java code depends on the Aerospike Java library, you can either download the demo package from the Aerospike [website](http://www.aerospike.com/docs/client/java/) or you can go to [GitHub](https://github.com/aerospike/aerospike-client-java) to install and follow the instructions. 

To run the Java example:
```
mvn install
java -jar target/ExpireBin-1.0-jar-with-dependencies.jar
```
The Java example class can also be used as a library. It provides method wrappers for
the underlying UDF apply calls. 

###UDF
For usage within UDFs, import the module as follows:
```
local exp_bin = require('expire_bin');
exp_bin.get(rec, bin);
exp_bin.put(rec, bin, val, bin_ttl, exp_create);
exp_bin.puts(rec, map {bin = "bin_name", val = 12, bin_ttl = 100});
exp_bin.touch(rec, map {bin = "bin_name", bin_ttl = 10});
exp_bin.clean(rec, bin);
```

#Implementation

Expire bins are map objects encapsulating the bin data and bin TTL. The bin operations for
expire bin perform retrieval and sending operations while checking the stored TTL 
to perform the expiration functionality. 

#Extensions

As there are a limited number of bins in Aerospike, in many situations it is better to use a Map
instead of collection of bins. An implementation that works with a Map instead of individual bins
would be a better solution.

Multiple operations per transaction, like setting multiple bins, are not yet supported. Adding
extra library functions would be excellent.

#Known Limitations
The current limitation stores maps,there is no support of secondary index with expire bins.
If a put call is made, it can overwrite a special expirable-bin.