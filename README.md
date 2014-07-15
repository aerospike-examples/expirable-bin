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
-c "register module 'expire_bin.lua'"
```

## Notice to Users
This module provides bin level data expiration for Aerospike using implementation
specific UDF bins. Any standard read/write operations using the Aerospike clients
will not be supported and may interfere with the functionality of this module.

## Usage
This module can be used from client calls or within other UDFs. For examples of client
calls, see the C and Java examples under ```src/c``` and ```src/java```.

For usage within UDFs, import the module as follows:
```
	local exp_bin = require('expire_bin');
	exp_bin.get(rec, bin);
	exp_bin.put(rec, bin, val, bin_ttl, exp_create);
	exp_bin.puts(rec, {"bin" = bin, "val" = val, "bin_ttl" = bin_ttl});
	exp_bin.touch(rec, {"bin" = bin, "bin_ttl" = bin_ttl});
	exp_bin.clean(rec, bin);
```

