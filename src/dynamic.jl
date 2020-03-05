# Dynamic thunk definitions

"A handle to the scheduler, used by dynamic thunks."
struct SchedulerHandle
    thunk_id::Int
    to_chan::RemoteChannel
    from_chan::RemoteChannel
end

"""
Thrown when the scheduler intentionally halts before it's finished processing
the DAG.
"""
struct SchedulerHaltedException <: Exception end

"Thrown when a dynamic thunk command causes an error within the scheduler."
struct SchedulerDynamicException <: Exception end

"Processor for commands received from workers."
function process_dynamic!(state)
    sch_task = Base.current_task()
    for tid in keys(state.worker_chans)
        in_chan, out_chan = state.worker_chans[tid]
        @async begin
            while true
                try
                    tid, cmd, data = take!(in_chan)
                    @debug "Processing $cmd for $tid"
                    process_dynamic!(state, tid, out_chan, Val(cmd), data)
                catch err
                    if err isa SchedulerHaltedException
                        Base.throwto(sch_task, err)
                        rethrow(err)
                    end
                    @warn "Error while processing dynamic command:"
                    Base.showerror(stderr, err)
                    Base.show_backtrace(stderr, catch_backtrace())
                end
            end
        end
    end
end
function process_dynamic!(state, tid, chan, cmd, data)
    # Fallback
    @warn "Received invalid dynamic command $cmd from thunk $tid: $data"
    throw(SchedulerDynamicException())
end

const WORKER_DYNAMIC_RESULTS = Dict{UUID,Any}()
const WORKER_DYNAMIC_CONDITION = Condition()

# Worker-to-scheduler communication helpers
sendto!(h::SchedulerHandle, cmd, data) =
    put!(h.to_chan, (h.thunk_id, cmd, data))
function sendrecv!(h::SchedulerHandle, cmd, data)
    uuid = uuid4()
    put!(h.to_chan, (h.thunk_id, cmd, (uuid, data)))
    fetch_result(uuid)
end
function fetch_result(uuid::UUID)
    while true
        if haskey(WORKER_DYNAMIC_RESULTS, uuid)
            result = WORKER_DYNAMIC_RESULTS[uuid]
            delete!(WORKER_DYNAMIC_RESULTS, uuid)
            return result
        end
        wait(WORKER_DYNAMIC_CONDITION)
    end
end

"Listener for results received from scheduler."
function listen_dynamic(chan)
    @async begin
        while true
            uuid, result = take!(chan)
            @debug "Got result (UUID $uuid): $result"
            WORKER_DYNAMIC_RESULTS[uuid] = result
            notify(WORKER_DYNAMIC_CONDITION)
        end
    end
end

"Gets the current thunk's ID."
getid(h::SchedulerHandle) = h.thunk_id

"Halts the scheduler immediately."
halt!(h::SchedulerHandle) = sendto!(h, :halt, nothing)
process_dynamic!(state, tid, chan, cmd::Val{:halt}, _) =
    throw(SchedulerHaltedException())

"Executes arbitrary code in the scheduler."
exec!(h::SchedulerHandle, ex) = sendrecv!(h, :exec, ex)
function process_dynamic!(state, tid, chan, cmd::Val{:exec}, (uuid, ex))
    try
        func = eval(ex)
        result = Base.invokelatest(func, state, tid)
        put!(chan, (uuid, result))
    catch err
        @warn "Error during exec:"
        Base.showerror(stderr, err)
        Base.show_backtrace(stderr, catch_backtrace())
        put!(chan, (uuid, err))
    end
end

"Queries the DAG for information."
query_dag(h::SchedulerHandle, queries) =
    sendrecv!(h, :query_dag, queries)
function process_dynamic!(state, tid, chan, cmd::Val{:query_dag}, (uuid, queries))
    thunks = Dict{Int,NamedTuple}()
    for query in queries
        id = query.kern
        t = state.thunk_dict[id]
        nt = get!(thunks, id) do
            # Seed default result
            NamedTuple()
        end
        if query.query == :inputs
            # FIXME: Provide handle to non-thunk args
            thunks[id] = merge(nt, (inputs=t.inputs,))
        else
            @warn "Invalid dynamic query: $(query.query)"
            throw(SchedulerDynamicException())
        end
    end
    put!(chan, (uuid, thunks))
end

struct DynamicAddThunkSpec
    f::String
    inputs::Tuple
    meta::Bool
    options
    dynamic::Bool
end
"Adds a thunk to the DAG."
function add_thunk!(h::SchedulerHandle, f::String, inputs...; meta=false,
                    options=nothing, dynamic=false)
    sendrecv!(h, :add_thunk, DynamicAddThunkSpec(f, inputs; meta=meta,
            options=options, dynamic=dynamic))
end
function process_dynamic!(state, tid, chan, cmd::Val{:add_thunk}, (uuid, spec))
    t = Thunk(Main.eval(Meta.parse(spec.f)), spec.inputs...; meta=spec.meta,
              options=spec.options, dynamic=spec.dynamic)
    # FIXME: Add t to scheduler
    put!(chan, (uuid, t.id))
end
