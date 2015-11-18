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




