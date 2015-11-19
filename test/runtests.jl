addprocs(4)
using ClusterDicts
using Base.Test

function test_d(d, pids, name, ktype, vtype)
    common_tests(d, pids, name, ktype, vtype)

    if isa(d, GlobalDict)
        # test it is the same everywhere
        (k, v) = getkv(100, ktype, vtype)
        d[k] = v
        (_, v) = getkv(101, ktype, vtype)
        d[k] = v
        @sync begin
            results=Bool[]
            for p in d.pids
                @async push!(results, v == remotecall_fetch((n, k)->ClusterDicts.directory[n][k], p, d.name, k))
            end
        end
        @test all(results)

        (k, _) = getkv(102, ktype, vtype)
        v = vtype in (ASCIIString, Any) ? ValueF(()->string(myid())) : ValueF(()->convert(vtype, myid()))
        d[k] = v

        @sync begin
            results=Bool[]
            for p in d.pids
                if vtype in (ASCIIString, Any)
                    @async push!(results, remotecall_fetch((D, K)-> myid() == convert(Int, parse(Float64, D[K])), p, d, k))
                else
                    @async push!(results, remotecall_fetch((D, K)-> myid() == convert(Int, D[K]), p, d, k))
                end
            end
        end
        @test all(results)

    else
        # test it is distributed properly
        for i in 1001:1100
            (k, v) = getkv(i, ktype, vtype)
            d[k] = v
        end

        @sync begin
            results=[]
            for p in d.pids
                @async push!(results, remotecall_fetch(n->length(ClusterDicts.directory[n]), p, d.name))
            end
        end
        @test sum(results) == length(d)

        results = Int[]
        for i in 1200:1299
            (k, _) = getkv(i, ktype, vtype)
            v = vtype in (ASCIIString, Any) ? ValueF(()->string(myid())) : ValueF(()->convert(vtype, myid()))
            d[k] = v

            if vtype in (ASCIIString, Any)
                push!(results, convert(Int, parse(Float64, d[k])))
            else
                push!(results, convert(Int, d[k]))
            end
        end

        (r, counts) = hist(results)
        for c in counts
           @test (c > 1 && c < 100)
        end
        @test convert(Int, maximum(r)) == maximum(d.pids)
    end
end

function getkv(i, ktype, vtype)
    k = ktype in (ASCIIString, Any) ? "$i" : convert(ktype, i)
    v = vtype in (ASCIIString, Any) ? "$i" : convert(vtype, i)
    (k, v)
end

function common_tests(d, pids, name, ktype, vtype)
    println("Testing : ", d)
    @test isempty(d) == true
    (k, v) = getkv(0, ktype, vtype)
    d[k] = v
    @test isempty(d) == false
    @test d[k] == v
    @test length(d) == 1

    (k, v) = getkv(1, ktype, vtype)
    d[k] = v
    @test length(d) == 2
    @test d[k] == v

    (k, v) = getkv(2, ktype, vtype)

    @test_throws Exception d[k]
    @test get(d, k, 1234) == 1234
    @test get!(d, k, v) == v
    @test get(d, k, 1234) == v

    newv = vtype in (ASCIIString, Any) ? "3" : convert(vtype, 3)
    d[k] = newv
    @test d[k] == newv

    if ktype != Any && vtype != Any
        @test_throws Exception gd[:TYPEERR] = v
        @test_throws Exception gd[k] = :TYPEERR
        @test_throws Exception gd[:TYPEERR] = :TYPEERR
    end

end


for pids in Any[PidsAll(), PidsWorkers(), [2,3], :default]
    for KV in Any[(Int, Int), (Int, Float64), (ASCIIString, ASCIIString), :default]
        for name in Any["foobar", :default]
            kwargs=[]
            name != :default && push!(kwargs, (:name, name))
            if KV != :default
                push!(kwargs, (:ktype, KV[1]))
                push!(kwargs, (:vtype, KV[2]))
            else
                KV = (Any, Any)
            end

            for dtype in [GlobalDict, DistributedDict]
                if pids == :default
                    d = dtype(; kwargs...)
                else
                    d = dtype(pids; kwargs...)
                end
                test_d(d, pids, name, KV[1], KV[2])
                delete!(d)
            end
        end
    end
end

println("Testing get/set from both participating and non-participating workers")
println()
for dtype in [GlobalDict, DistributedDict]
    d = dtype([2,3]) # dict on subset of workers

    i = 1
    # test get/set from both participating and non-participating workers
    for p in procs()
        i = i+1; k = i; v = string(i)
        d[k] = v
        @test v == remotecall_fetch((D, K)->D[K], p, d, k)

        i = i+1; k = i; v = string(i)
        remotecall_fetch((D, K, V)->(D[K]=V; nothing), p, d, k, v)
        @test d[k] == v
    end

    delete!(d)
end


# ensure that the directory is empty on all nodes
println("Test for any leaks")
println()
results=Bool[]
for p in procs()
    push!(results, remotecall_fetch(()->isempty(ClusterDicts.directory), p))
end

@test all(results)




