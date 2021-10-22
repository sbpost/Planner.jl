module Planner
export startplanning, addtarget!, runplan

using Graphs
using DataFrames

mutable struct Plan
    G::SimpleDiGraph
    graph_data::DataFrame
end

"""
    a_plan = startplanning()

Initialise a Plan to track the project dependencies (inputs -> outputs
 relationships). The functiion simply outputs an empty Plan-object with a graph
 G, and an associated dataframe, graph_data.
"""
function startplanning()
    G = SimpleDiGraph()
    graph_data = DataFrame(index = Int[], filename = String[])
    return Plan(G, graph_data)
end

"""
    addtarget!(plan, outputs, inputs)

Add a element to the plan (inputs -> output) defined by `G` and `graph_data`.

# Arguments
* `plan`: The plan (::Plan) that represents the dependency graph. The input-
output combination is added to this plan.
* `outputs`: The files that results from the inputs (scripts, data).
* `depends_on`: The files that creates the outputs. The outpus must be created
by running the .jl scripts in `depends_on`.

# Examples
```julia
new_plan = startplanning()
addtarget!(new_plan,
           ["files/someoutput.csv"],
           ["files/someinputdata.txt", "somescript.jl"])
```
"""
function addtarget!(plan::Plan, outputs::Array{String}, depends_on::Array{String})
    # Check if inputs, outputs are already in graph:
    # If yes, just add edges. If no, first add as vertices.
    not_in_graph = [file for file in vcat(depends_on, outputs) if !hasnode(plan, file)]
    length(not_in_graph) > 0 ? addnode!.(Ref(plan), not_in_graph) : nothing

    # Add edges:
    input_indices = filter(row -> row.filename ∈ depends_on,
                           plan.graph_data).index
    output_indices = filter(row -> row.filename ∈ outputs,
                            plan.graph_data).index
    for i in input_indices
        for o in output_indices
            # Does edge exit?
            has_edge(plan.G, i, o) ? nothing : add_edge!(plan.G, i, o)
        end
    end
end

"""
    hasnode(plan::Plan, filename::String)

Check if the dependency graph has a node with the given filename attached.
"""
function hasnode(plan::Plan, filename::String)
    return filename ∈ plan.graph_data.filename
end

"""
    addnode(plan::Plan, filename::String; kwargs...)

Add a node to the dependency graph. Any key word arguments are added
as properties to the node (and stored in plan.graph_data).
"""
function addnode!(plan::Plan, filename::String; kwargs...)
    # Make sure filename is not already in G:
    @assert !(hasnode(plan, filename))

    # Update graph:
    add_vertex!(plan.G)

    # Set properties;
    properties = Dict{Symbol, Any}(kwargs)
    properties[:index] = nv(plan.G)
    properties[:filename] = filename

    # Update data:
    push!(plan.graph_data, properties, cols=:union)
end

function getproperty(v::Int, plan::Plan, property::Symbol)
    @assert v ∈ plan.graph_data.index
    @assert string(property) ∈ names(plan.graph_data)
    return filter(row -> row.index == v, plan.graph_data)[!, property] |> only
end

function getindex(value::Any, plan::Plan, property::Symbol)
    @assert string(property) ∈ names(plan.graph_data)
    return filter(row -> row[property] == value, plan.graph_data)[!, :index] |> only
end

"""
    runplan(plan::Plan)

Run the plan defined by the dependency graph `G` and the associated information
 `graph_data`.

```julia
plan = startplanning()

addtarget!(plan,
           ["files/file_4.txt"], # output
           ["files/file_1.txt", "files/file_2"]) # depends on

addtarget!(plan,
           ["files/file_5.txt"], # output
           ["files/file_4.txt", "files/file_3.txt"]) # depends on

runplan(plan)
```
"""
function runplan(plan::Plan)
    eval_order, is_stale = staletargets(plan)

    while sum(is_stale) > 0
        target = eval_order[is_stale] |> first
        @info "Updating target: $target."
        rundeps(target, plan)
        eval_order, is_stale = staletargets(plan)
    end
    @info "The plan is up to date."
end

"""
    uptodate(node::Int, plan::Plan)

Check if a given node is newer than its immediate dependencies.
"""
function uptodate(node::Int, plan::Plan)
    dependencies = inneighbors(plan.G, node)
    # if node has no dependencies, it is always up to date
    length(dependencies) == 0 && return true

    dependency_change_times = getproperty.(dependencies, Ref(plan), :change_time)
    if any(getproperty(node, plan, :change_time) .< dependency_change_times)
        return false
    else
        return true
    end
end

function updatechangetimes!(plan::Plan)
    plan.graph_data[!, :change_time] = ctime.(plan.graph_data.filename)
end

"""
    staletargets(plan::Plan)

Get a tuple containing the evaluation order and which of these nodes are stale
 (i.e. have dependencies newer than outputs).
"""
function staletargets(plan::Plan)
    eval_order = topological_sort_by_dfs(plan.G)
    updatechangetimes!(plan)
    is_stale = .!uptodate.(eval_order, Ref(plan))
    return eval_order, is_stale
end

"Run the dependencies for a given `node` in the `plan`."
function rundeps(node::Int, plan::Plan)
    dependencies = inneighbors(plan.G, node)
    dependency_files = getproperty.(dependencies, Ref(plan), :filename)
    # Only run julia scripts. Data should be updated by scripts.
    for filename in dependency_files
        if last(filename, 3) == ".jl"
            @info "Running file: $(filename)."
            include(filename)
        end
    end
end

end # module
