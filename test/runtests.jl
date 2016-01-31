using CQL
using Base.Test

include("examples.jl")
# write your own tests here
@test cqltest01()
@test cqltest02()
@test cqltest03()
