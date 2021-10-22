module Planner

using Graphs
using DataFrames

"Add a inputs->outputs combination to the dependency graph."
"""
    add_taget!(G, graph_data, ["output1.txt"], ["input1.txt", "input2.txt"])

Add a element to the plan (inputs -> output) defined by `G` and `graph_data`.

# Arguments
* `G`: The graph-object that represent the dependency graph.
* `graph_data`: The dataframe that contains the information associated with nodes in `G`
* `outputs`: The files that results from the inputs (scripts, data).
* `depends_on`: The files that creates the outputs. The outpus must be created by running
the .jl scripts in `depends_on`.

"""
function add_target!(G::SimpleDiGraph{Int64}, graph_data::DataFrame, outputs::Array{String}, depends_on::Array{String})
    # Check if inputs, outputs are already in graph:
    # If yes, just add edges. If no, first add as vertices.
    not_in_graph = [file for file in vcat(depends_on, outputs) if !has_node(graph_data, file)]
    length(not_in_graph) > 0 ? add_node!.(Ref(G), Ref(graph_data), not_in_graph) : nothing

    # Add edges:
    input_indices = filter(row -> row.filename ∈ depends_on, graph_data).index
    output_indices = filter(row -> row.filename ∈ outputs, graph_data).index
    for i in input_indices
        for o in output_indices
            # Does edge exit?
            has_edge(G, i, o) ? nothing : add_edge!(G, i, o)
        end
    end
end

"Check if the dependency graph has a node with the given filename attached."
function has_node(graph_data::DataFrame, filename::String)
    return filename ∈ graph_data.filename
end

"Add a node to the dependency graph."
function add_node!(G::SimpleDiGraph{Int64}, graph_data::DataFrame, filename::String; kwargs...)
    # Make sure filename is not already in G:
    @assert !(has_node(graph_data, filename))

    # Update graph:
    add_vertex!(G)

    # Set properties;
    properties = Dict{Symbol, Any}(kwargs)
    properties[:index] = nv(G)
    properties[:filename] = filename

    # Update data:
    push!(graph_data, properties, cols=:union)
end

function get_property(v::Int, graph_data::DataFrame, property::Symbol)
    return filter(row -> row.index == v, graph_data)[!, property] |> only
end

"""
Run the plan defined by the dependency graph `G` and the associated information `graph_data`.

```julia
G = SimpleDiGraph()

add_target!(G, graph_data,
           ["files/file_4.txt"], # output
           ["files/file_1.txt", "files/file_2"]) # depends on

add_target!(G, graph_data,
           ["files/file_5.txt"], # output
           ["files/file_4.txt", "files/file_3.txt"]) # depends on

run_plan(G, graph_data)
```
"""
function run_plan(G::SimpleDiGraph, graph_data::DataFrame)
    updates = get_update_schedule(G, graph_data)
    for filename in updates[updates .!= nothing]
        # Only run julia scripts. Data should be updated by scripts.
        if last(filename, 3) == ".jl"
            @info "Running file $(filename)."
            include(filename)
        end
    end
end

function get_update_schedule(G::SimpleDiGraph, graph_data::DataFrame)
    evaluation_order = topological_sort_by_dfs(G)
    update_change_times!(graph_data)
    return check_dependencies.(evaluation_order, Ref(G), Ref(graph_data))
end

function check_dependencies(node::Int, G::SimpleDiGraph, graph_data::DataFrame)
    dependencies = inneighbors(G, node)
    # if node has no dependencies, go to next node
    length(dependencies) == 0 && return nothing
    dependency_change_times = get_property.(dependencies, Ref(graph_data), :change_time)
    if any(get_property(node, graph_data, :change_time) .< dependency_change_times)
        return get_property(node, graph_data, :filename)
    else
        return nothing
    end
end

function update_change_times!(graph_data::DataFrame)
    graph_data[!, :change_time] = ctime.(graph_data.filename)
end

end # module
