Expirable-bin
=============

Provide bin-level expiration functionlaity in Aerospike, using User Defined Functions.

The module provides:
record.put - insert bins with optional time-to-live.
record.get - return bins that are not expired.
scan - scan the database, rewrite records which expired bins.

Client interface is available for Java, C, Python, and Lua

## Installation

The source for this module is available on Github, at https://github.com/aerospike/expirable-bin.git
Clone the Github repository using the following command:
```
git clone https://github.com/aerospike/expirable-bin.git
```

Register the lua file using aql,
```aql
aql -c "register module 'expire_bin.lua'"
```

## Usage
This module can be used from client calls or within other UDFs. For examples of client
calls, see the C and Java examples under ```src/c``` and ```src/java```. Please ensure
that the expire_bin.lua file is registered to the server before running the examples.

To run the java example,
```
	mvn install
	java -jar target/ExpireBin-1.0-jar-with-dependencies.jar
```
The java example class can also be used as a library. It provides method wrappers for
the underlying UDF apply calls. 

For usage within UDFs, import the module as follows:
```
	local exp_bin = require('expire_bin');
	exp_bin.get(rec, bin);
	exp_bin.put(rec, bin, val, bin_ttl, exp_create);
	exp_bin.puts(rec, map {bin = "bin_name", val = 12, bin_ttl = 100});
	exp_bin.touch(rec, map {bin = "bin_name", bin_ttl = 10});
	exp_bin.clean(rec, bin);
```
## Implementation
Expire bins are map objects encapsulating the bin data and bin TTL. The bin operations for
expire bin perform retrieval and sending operations while checking the stored TTL 
to perform the expiration functionality. 

## Known Limitations
The current limitation stores maps,there is no support of secondary index with expire bins.
If a put call is made, it can overwrite a special expirable-bin.
