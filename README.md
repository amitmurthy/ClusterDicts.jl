# ClusterDicts

[![Build Status](https://travis-ci.org/amitmurthy/ClusterDicts.jl.svg?branch=master)](https://travis-ci.org/amitmurthy/ClusterDicts.jl)
[![codecov.io](http://codecov.io/github/amitmurthy/ClusterDicts.jl/coverage.svg?branch=master)](http://codecov.io/github/amitmurthy/ClusterDicts.jl?branch=master)

[![ClusterDicts](http://pkg.julialang.org/badges/ClusterDicts_0.4.svg)](http://pkg.julialang.org/?pkg=ClusterDicts&ver=0.4)
---

Global and Distributed dictionaries for Julia.

GlobalDict and DistributedDict
---------------------------

This package provides two types of distributed dictionaries:

- `GlobalDict` where the same key-value pair is stored on
    all participating workers.
- `DistributedDict` where the key-value pairs are distributed over
    the participating workers based on the key's hash value.

Constructors
------------

- `GlobalDict(pids=PidsWorkers(); name::AbstractString=next_name(), ktype=Any, vtype=Any)` where
    - `pids` is one of `PidsAll()`, `PidsWorkers()` or `Pids([ids...])` where
        - `PidsAll()` represents all processes in the cluster
        - `PidsWorkers()` represents all worker processes in the cluster
        - `Pids([ids...])` represents a specific subset of workers identified by their pids.

    - `name` is a logical name for the dictionary. It should be unique across any such dictionary created across
    modules or different packages. Usually left unspecified in which case the system creates a unique name.

    - `ktype` is the type of the keys. Defaults to `Any`.

    - `vtype` is the type of the values. Defaults to `Any`.

- `DistributedDict(pids=PidsWorkers(); name::AbstractString=next_name(), ktype=Any, vtype=Any)`
  similar to the `GlobalDict` constructor.

- `GlobalDict()` and `DistributedDict()` only distribute over worker processes, i.e., excluding the master.
   Keyword args remain the same.

- `GlobalDict(pids::Array)` and `DistributedDict(pids::Array)` distribute on the specified pids.
  Keyword args remain the same.


Functions
---------

The following functions of `Associative` are implemented:

- `isempty`
- `length`
- `get`
- `get!`
- `getindex`
- `setindex!`
- `pop!`
- `delete!`

ValueF
------

Any closure wrapped in a ValueF will execute the closure on the node where the key is being assigned.

For example, for a `GlobalDict`,
```
    d[k] = ValueF(()->myid())
```

will set the value of d[k] to 2 on pid 2, 3 on pid 3 and so on.

And for a `DistributedDict`, it will be set only on the node that the key hashes to.


Garbage Collection
------------------

The global and distributed dictionaries created by `GlobalDict` and `DistributedDict` are NOT collected when the objects go out of scope.
They have to be necessarily released by calling `delete!(d::GlobalDict)` and `delete!(d::DistributedDict)` respectively.


TODO
----

- Concurrent updates : This current version makes no attempt to handle race conditions when multiple tasks or processes
update the same key. For example, if `d` is a `GlobalDict`, `d[k] = v` sets `k` to `v` on all participating workers.
If two tasks try to set `d[k] = v1` and ``d[k] = v2` at the same time, there is no guarantee the value will be either
all `v1` or all `v2` on all workers.

- More `Associative` functions to be implemented.

- Handle worker exits : Resiliency in the event of workers exiting has to be handled.


