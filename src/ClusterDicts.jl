VERSION >= v"0.4.0-dev+6521" && __precompile__(true)

module ClusterDicts
using Compat

export GlobalDict, DistributedDict, Pids, PidsAll, PidsWorkers

abstract AbstractPids

type Pids <: AbstractPids
    pids::Array
end

type PidsAll <: AbstractPids
end

type PidsWorkers <: AbstractPids
end

Base.in(x, pids::Pids) = x in pids.pids
Base.in(x, pids::PidsAll) = x in procs()
Base.in(x, pids::PidsWorkers) = x in workers()

Base.start(d::Pids) = (1, d.pids)
Base.start(::PidsAll) = (1, procs())
Base.start(::PidsWorkers) = (1, workers())

Base.length(d::Pids) = length(d.pids)
Base.length(::PidsAll) = nprocs()
Base.length(::PidsWorkers) = nworkers()

Base.getindex(d::Pids, idx::Int) = d.pids[idx]
Base.getindex(::PidsAll, idx::Int) = procs()[idx]
Base.getindex(::PidsWorkers, idx::Int) = workers()[idx]

function Base.done(::AbstractPids, state)
    (idx, pids) = state
    return idx > length(pids)
end

function Base.next(::AbstractPids, state)
    (idx, pids) = state
    return (pids[idx], (idx+1, pids))
end

let DID = 0
    global next_name
    function next_name()
        DID = DID + 1
        return string(randstring(), '_', myid(), '_', DID)
    end
end

const directory = Dict()

function init_dict_local(n, K, V, dtype)
    global directory
    if haskey(directory, n)
        existing = directory[(n, :dtype)]
        if (existing[1] == K) && (existing[2] == V) && (existing[3] == dtype)
            return :OK
        else
            return :EXISTS
        end
    else
       directory[n] = Dict{K,V}()
       directory[(n, :dtype)] = (K, V, dtype)
       return :OK
    end
end

function delete_dict_local(n)
    global directory
    delete!(directory, n)
    nothing
end

function init_dict(pids, n, K, V, dtype)
    results = []
    @sync begin
        for p in pids
            @async push!(results, (p, remotecall_fetch(init_dict_local, p, n, K, V, dtype)))
        end
    end

    err = []
    ok = []
    map(x -> x[2] == :EXISTS ? push!(err, x[1]) : push!(ok, x[1]), results)

    if length(err) > 0
        # cleanup newly created dicts
        @sync begin
            for p in ok
                @async remotecall_fetch(delete_dict_local, p, n)
            end
        end

        error("A Cluster Dictionary with name $n already exists.")
    end
end

abstract DistributedAssociative{K,V} <: Associative{K,V}

for DT in [:GlobalDict, :DistributedDict]
    @eval type $DT{K,V} <: DistributedAssociative{K,V}
            name::AbstractString
            pids::AbstractPids

            function $DT(name::AbstractString, pids::AbstractPids, ktype, vtype)
                init_dict(pids, name, ktype, vtype, $DT)
                new(name, pids)
            end
        end

    @eval $DT(;kwargs...) = $DT(PidsWorkers(); kwargs...)
    @eval function $DT(pids::Array; name=next_name(), ktype=Any, vtype=Any)
            if pids == procs()
                return $DT(PidsAll(); name=name, ktype=ktype, vtype=vtype)
            elseif pids == workers()
                return $DT(PidsWorkers(); name=name, ktype=ktype, vtype=vtype)
            else
                return $DT(Pids(pids); name=name, ktype=ktype, vtype=vtype)
            end
        end

    @eval $DT(pids::AbstractPids; name=next_name(), ktype=Any, vtype=Any) = $DT{ktype, vtype}(name, pids, ktype, vtype)

    @eval Base.show(io::IO, d::$DT) = println(typeof(d), "(", d.name, ",", d.pids, ")")
end

where(d::DistributedDict, k) = d.pids[Int(hash(k) % length(d.pids) + 1)]
function where(d::GlobalDict, k=nothing)
    if myid() in d.pids
        return myid()
    else
        return d.pids[rand(1:length(d.pids))]   # Pick a random worker where this dict exists
    end
end

Base.haskey(d::DistributedAssociative, k) = remotecall_fetch((n, k)->(global directory; haskey(directory[n], k)), where(d, k), d.name, k)

import Base.get, Base.get!
export get, get!
for (f, f_local, insert) in Any[(:get, :get_local, false), (:get!, :get!_local, true)]
    @eval function ($f_local)(name, k)
            global directory
            kd = directory[name]
            if haskey(kd, k)
                return (:OK, kd[k])
            else
                return :NOTFOUND
            end
        end

    @eval function ($f)(d::DistributedAssociative, k, default)
            pid = where(d,k)
            if myid() == pid
                return ($f)(directory[d.name], k, default)
            else
                response = remotecall_fetch(($f_local), pid, d.name, k)
                if response == :NOTFOUND
                    if $insert
                        d[k] = default
                    end
                    return default
                else
                    (_, v) = response
                    return v
                end
            end
        end
end


function pop_impl!(d::DistributedAssociative, k, f)
    response = remotecall_fetch(get_local, where(d,k), d.name, k)
    if response == :NOTFOUND
        return f()
    else
        delete!(d, k)   # delete from everywhere
        (_, v) = response
        return v
    end
end
Base.pop!(d::DistributedAssociative, k) = pop_impl!(d, k, ()->throw(KeyError("No mapping found for key : ", string(k))))
Base.pop!(d::DistributedAssociative, k, default) = pop_impl!(d, k, ()->default)

setindex!_local(name, k, v) = (global directory; directory[name][k] = v; nothing)

function Base.setindex!(d::GlobalDict, v, k)
    @sync begin
        for p in d.pids
            @async remotecall_fetch(setindex!_local, p, d.name, k, v)
        end
    end
    d
end
Base.setindex!(d::DistributedDict, v, k) = remotecall_fetch(setindex!_local, where(d, k), d.name, k, v)


getindex_local(name, k) = (global directory; return directory[name][k])
Base.getindex(d::DistributedAssociative, k) = remotecall_fetch(getindex_local, where(d,k), d.name, k)

delete!_local(name, k) = (global directory; delete!(directory[name], k); nothing)
Base.delete!(d::GlobalDict, k) = (collect_from_everywhere(d, delete!_local, d.name, k); d)
Base.delete!(d::DistributedDict, k) = remotecall_fetch(delete!, where(d,k), d.name, k)

isempty_local(name) = (global directory; isempty(directory[name]))
Base.isempty(d::GlobalDict) = remotecall_fetch(isempty_local, where(d), d.name)
Base.isempty(d::DistributedDict) = reduce(&, collect_from_everywhere(d, isempty_local, d.name))

length_local(name) = (global directory; length(directory[name]))
Base.length(d::GlobalDict) = remotecall_fetch(length_local, where(d), d.name)
Base.length(d::DistributedDict) = sum(collect_from_everywhere(d, length_local, d.name))

function collect_from_everywhere(d::DistributedAssociative, f, args...)
    results=Any[]
    @sync begin
        for p in d.pids
            @async push!(results, remotecall_fetch(f, p, args...))
        end
    end
    results
end

delete!_local(name) = (global directory; delete!(directory, name); nothing)
Base.delete!(d::DistributedAssociative) = (collect_from_everywhere(d, delete!_local, d.name); nothing)

end # module
