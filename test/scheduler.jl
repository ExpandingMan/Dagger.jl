import Dagger.Sch: SchedulerOptions, ThunkOptions, SchedulerHaltedException
import Dagger.Sch: halt!

@everywhere begin
using Dagger
function checkwid(x...)
    @assert myid() == 1
    return 1
end
function checktid(x...)
    @assert Threads.threadid() != 1 || Threads.nthreads() == 1
    return 1
end
function dynamic_halt(h, x)
    Dagger.Sch.halt!(h)
    return x
end
end

@testset "Scheduler" begin
    @testset "Scheduler options: single worker" begin
        options = SchedulerOptions(;single=1)
        a = delayed(checkwid)(1)
        b = delayed(checkwid)(2)
        c = delayed(checkwid)(a,b)

        @test collect(Context(), c; options=options) == 1
    end
    @testset "Thunk options: single worker" begin
        options = ThunkOptions(;single=1)
        a = delayed(checkwid; options=options)(1)

        @test collect(Context(), a) == 1
    end
    @static if VERSION >= v"1.3.0-DEV.573"
        if Threads.nthreads() == 1
            @warn "Threading tests running in serial"
        end
        @testset "Scheduler options: threads" begin
            options = SchedulerOptions(;threads=true)
            a = delayed(checktid)(1)
            b = delayed(checktid)(2)
            c = delayed(checktid)(a,b)

            @test collect(Context(), c; options=options) == 1
        end
        @testset "Thunk options: threads" begin
            options = ThunkOptions(;threads=true)
            a = delayed(checktid; options=options)(1)

            @test collect(Context(), a) == 1
        end
    end
end

@testset "Dynamic Thunks" begin
    @testset "Scheduler control" begin
        a = delayed(dynamic_halt; dynamic=true)(1)
        @test_throws SchedulerHaltedException collect(Context(), a)
    end
end
