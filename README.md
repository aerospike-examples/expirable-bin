expirable_bin
=============

Managing bins with time-to-live in Aerospike, using User Defined Functions.

## Installation

The source for this module is available on Github, at https://github.com/aerospike/expirable_bin.git
Clone the Github repository using the following command:
```
git clone https://github.com/aerospike/expirable_bin.git
```

Register the lua file using aql,
```aql
aql -c "register module 'expire_bin.lua'"
```

## Notice to Users
This module provides bin level data expiration for Aerospike using implementation
specific UDF bins. Any standard read/write operations using the Aerospike clients
will not be supported and may interfere with the functionality of this module.

## Usage
This module can be used from client calls or within other UDFs. For examples of client
calls, see the C and Java examples under ```src/c``` and ```src/java```. Please ensure
that the expire_bin.lua file is registered to the server before running the examples.

For usage within UDFs, import the module as follows:
```
	local exp_bin = require('expire_bin');
	exp_bin.get(rec, bin);
	exp_bin.put(rec, bin, val, bin_ttl, exp_create);
	exp_bin.puts(rec, map {bin = "bin_name", val = 12, bin_ttl = 100});
	exp_bin.touch(rec, map {bin = "bin_name", bin_ttl = 10});
	exp_bin.clean(rec, bin);
```
## Architecture
Expire bins are map objects encapsulating the bin data and bin TTL. The bin operations for
expire bin perform retrieval and sending operations while checking the stored metadata 
to perform the expiration functionality. Because the bins are essentially storing maps,
there is no support of secondary index with expire bins yet. 
