include("factory_simulation.jl")

### Simulation Harness
#number of realisations
M = 100
rerun = true # rerun all simulations
output = false #produce output for simulations
if output == false
    rerun = true
end
Time_limit = [100.0, 1_000.0, 10_000.0, 100_000.0]
N = length(Time_limit)
perf_times = zeros(M,N) # initialise a vector to hold performance run times of the simulation
# Define path for Parameter_set data file
parameter_path = pwd()*"/data/factory_simulation/" # directory name
#read the csv file 
parameter_set = read_parameters(parameter_path*"parameter_set.csv")
#Generate M realisations of the simulation
for seed = 1:M
    for j = 1:N
    # for the given parameters in the set of parameters 
    for parameter in eachrow(parameter_set)
        # define the output files        
        (file_entities,file_state) = output_file(seed, parameter,output)
        # Run the simulation based on the flag or missing data files for particular seeds and parameters
        if rerun || !(isfile(file_entities) && isfile(file_state))
           times[seed,j] = @elapsed run_factory_simulation(seed, Time_limit[j], parameter, output, file_state, file_entities)
        end
        end
    end
end

#display time
display(times)