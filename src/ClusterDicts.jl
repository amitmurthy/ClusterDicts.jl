VERSION >= v"0.4.0-dev+6521" && __precompile__(true)

module ClusterDicts
using Compat

export GlobalDict, DistributedDict, Pids, PidsAll, PidsWorkers, ValueF

abstract AbstractPids

type Pids <: AbstractPids
    pids::Array
end

type PidsAll <: AbstractPids end
type PidsWorkers <: AbstractPids end

type ValueF
    f::Function
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

function exists(pids, name, K, V, dtype)
    assert(myid() == 1)

    global directory
    D = get(directory, (name, :dtype), nothing)
    D == nothing && return (false, false)

    if (D.pids == pids) && (keytype(D) == K) && (valtype(D) == V) && isa(D, dtype)
        return (true, true)
    else
        return (true, false)
    end
end

function init_dict_local(pids, name, K, V, dtype)
    if (myid() in pids)
        directory[name] = Dict{K,V}()
    end
    directory[(name, :dtype)] = dtype{K, V}(name, pids)
    nothing
end

function init_dict(pids, name, K, V, dtype)
    # Ensure no dict with the same name but different specifications exists
    (name_exists, specs_exists) = remotecall_fetch(exists, 1, pids, name, K, V, dtype)
    if specs_exists
        return true
    elseif name_exists
        error("A cluster dictionary with name " * name * " but different specifications already exists.")
    end

    # Instantiate on 1 first. Master holds meta data for all dicts.
    remotecall_fetch(init_dict_local, 1, pids, name, K, V, dtype)

    @sync begin
        for p in filter(x->x!=1, pids)
            @async remotecall_fetch(init_dict_local, p, pids, name, K, V, dtype)
        end
    end

    return false
end

abstract DistributedAssociative{K,V} <: Associative{K,V}

for DT in [:GlobalDict, :DistributedDict]
    @eval type $DT{K,V} <: DistributedAssociative{K,V}
            name::AbstractString
            pids::AbstractPids

            function $DT(name::AbstractString, pids::AbstractPids, ktype, vtype)
                pre_exists = init_dict(pids, name, ktype, vtype, $DT)
                pre_exists && warn("A cluster dictionary with name " * name * " and similar specification exists.")
                if (myid()==1) || (myid() in pids)
                    return directory[(name, :dtype)]
                else
                    $DT(name, pids)
                end
            end

            $DT(name::AbstractString, pids::AbstractPids) = new(name, pids)
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

    @eval function $DT(name::AbstractString)
            # this method expects the directory to have an entry
            return directory[(name, :dtype)]
        end

    @eval Base.show(io::IO, d::$DT) = println(typeof(d), "(", d.name, ",", d.pids, ")")
end

function serialize(S::SerializationState, d::DistributedAssociative)
    p = Base.worker_id_from_socket(s.io)
    Serializer.serialize_type(S, typeof(d))

    # Optimize ser/deser by sending pids array only if the destination worker does
    # not have it.

    if (p in rr.pids) || (p == 1)
        serialize(S, (d.name, nothing))
    else
        serialize(S, (d.name, d.pids))
    end
end

function deserialize{T<:DistributedAssociative}(S::SerializationState, t::Type{T})
    global directory
    (name, pids) = deserialize(S)
    if pids == nothing
        # we expect the local directory to have an entry
        return T(name)
    else
        return T(name, pids)
    end
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
setindex!_local(name, k, v::ValueF) = (global directory; directory[name][k] = v.f(); nothing)

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

function delete!_local(name)
    global directory
    delete!(directory, name)
    delete!(directory, (name, :dtype))
    nothing
end

function Base.delete!(d::DistributedAssociative)
    collect_from_everywhere(d, delete!_local, d.name);
    if !(1 in d.pids)
        remotecall_fetch(delete!_local, 1, d.name)
    end
end

end # module
