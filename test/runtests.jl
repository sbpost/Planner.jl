using Test, Planner

# Test startplanning
test_plan = startplanning()
@test typeof(test_plan) == Plan

@test typeof(test_plan.G) == SimpleDiGraph{Int64}
@test nv(test_plan.G) == 0 && ne(test_plan.G) == 0

@test typeof(test_plan.graph_data) == DataFrame
@test size(test_plan.graph_data) == (0, 2)

# TODO Test basic functionality:
# add node
tp = startplanning()
addnode!(tp, "testnode1.csv")
addnode!(tp, "testnode2.csv")
@test nv(tp.G) == 2 && ne(tp.G) == 0
@test size(tp.graph_data) == (2, 2)
@test tp.graph_data.filename == ["testnode1.csv", "testnode2.csv"]
@test tp.graph_data.index == [1, 2]
@test_throws AssertionError addnode!(tp, "testnode2.csv")

# has node
tp = startplanning()
addnode!(tp, "testnode1.csv")
@test hasnode(tp, "testnode1.csv")
@test !hasnode(tp, "testnode2.csv")

# TODO add property

# get property
tp = startplanning()
addnode!(tp, "testnode1.csv")
@test getproperty(1, tp, :filename) == "testnode1.csv"
@test_throws AssertionError getproperty(1, tp, :note) # property does not exist
@test_throws AssertionError getproperty(2, tp, :filename) # node does not exit

# add target
tp = startplanning()
addtarget!(tp,
           ["someoutput.csv"],
           ["someinput.txt", "somescript.jl"])

@test nv(tp.G) == 3
# make sure edges have been made
@test collect(edges(tp.G)) == [Edge(1 => 3), Edge(2 => 3)]
# make sure edges are between correct nodes
@test getproperty.([1, 2, 3], Ref(tp), :filename) == ["someinput.txt", "somescript.jl", "someoutput.csv"]

# change times and immediate dependencies
tp = startplanning()
# Create some test files:
tempfiles = ["./testfile$i.txt" for i in 1:5]
touch.(tempfiles)

# Build graph:
# 1------> 5
# 2-->4--/
# 3-/
addtarget!(tp,
           ["./testfile4.txt"],
           ["./testfile2.txt", "./testfile3.txt"])

addtarget!(tp,
           ["./testfile5.txt"],
           ["./testfile1.txt", "./testfile4.txt"])

@test "change_time" ∉ tp.graph_data |> names
update_change_times!(tp)
@test "change_time" ∈ tp.graph_data |> names

# Nothing should need to be updated.
update_change_times!(tp)
@test uptodate(get_index("./testfile4.txt", tp, :filename), tp)
@test uptodate(get_index("./testfile5.txt", tp, :filename), tp)
# Now testfile4 and testfile_5 should need to be updated:
touch("./testfile1.txt")
touch("./testfile2.txt")
update_change_times!(tp)
@test !uptodate(get_index("./testfile5.txt", tp, :filename), tp)
@test !uptodate(get_index("./testfile4.txt", tp, :filename), tp)

# get update_schedule:
@test typeof(get_schedule(tp)) == Vector{Int}
@test getproperty.(get_schedule(tp), Ref(tp), :filename) == ["./testfile4.txt", "./testfile5.txt"]
new_plan = startplanning()
@test staletargets(new_plan)|> length == 0

# clean up:
rm.(tempfiles)

# run plan
tp = startplanning()

tempscripts = ["./t1.jl",
               "./t2.jl",
               "./t3.jl"]
tempdata = ["./in1.csv",
            "./in2.csv",
            "./in3.csv"]

touch.(vcat(tempdata, tempscripts))

# Build graph:
#      out2           out3
#      /  | \          | \
#    out1 |  \         |  \
#  /  |   |   \        |   \
# in1 t1 in2  t2      in3  t3


addtarget!(tp,
           ["./out1.csv"],
           ["./t1.jl", "./in1.csv"])

addtarget!(tp,
           ["./out2.csv"],
           ["./out1.csv", "./t2.jl", "./in2.csv"])

addtarget!(tp,
           ["./out3.csv"],
           ["./in3.csv", "./t3.jl"])

touch("./out1.csv")
touch("./out2.csv")
touch("./out3.csv")

# Get all targets that are stale:
# Get inputs for all of these targets:

# run plan:
# recursively get stale targets and evaluation order, run first stale targets dependencies.
# this should fix the problem with non-connected graphs, and only stuff that needs to be run will.
# given that runtime is much slower than planner time, this shouldnt matter.

touch("./t1.jl")

# This is the setup:
eval_order, is_stale = staletargets(tp)
# Get dependencies of first stale target:
target = eval_order[is_stale] |> first
dependencies = inneighbors(tp.G, target)
dependency_files = getproperty.(dependencies, Ref(tp), :filename)
# Run dependencies (that will re-create the new file.)
touch.(dependency_files)
touch.(getproperty(target, tp, :filename))

