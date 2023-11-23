using DataStructures
using Distributions
using StableRNGs
using Dates
using CSV
using DataFrames

### Entity data structure for each Lawnmower
mutable struct Lawnmower
    id::Int64                        # a unique id to be allocated upon arrival
    arrival_time::Float64          # the time the lawnmower arrived in production from the start of the simulation
    start_service_time::Float64    # the time the blade machine serves the lawnmower (from start of simulation)
    completion_time::Float64       # the time the blade machine finished (from start of simulation)
    interrupted::Int64             # whether the lawnmower construction was interrupted or not.
    interruption_duration::Float64 # the duration the lawnmower construction was interrupted.
end

### Event data structures
abstract type Event end 
# Arrival event
struct OrderArrival <: Event # a lawnmower is ordered
    id::Int64         # a unique event id
    time::Float64     # the time of the event 
end

# Departure event
mutable struct OrderComplete <: Event # a lawnmower is completed construction
    id::Int64         # a unique event id
    time::Float64     # the time of the event
end

# Vacation event
struct MachineBreakdown <: Event # the blade-fitting machine breaks down and undergoes repair
    id::Int64         # a unique event id
    time::Float64     # the time of the event
end

# Resume service event
struct MachineResume <: Event # the blade-fitting machine resumes construction 
    id::Int64         # a unique event id
    time::Float64     # the time of the event
end

struct Null <: Event 
    id::Int64    
end

### State data structure 
mutable struct SystemState
    time::Float64                               # the system time (simulation time)
    n_lawnmowers::Int64                         # the number of lawnmowers (entities) to have been constructed
    event_queue::PriorityQueue{Event,Float64}   # to keep track of future events
    production_line_queue::Queue{Lawnmower}     # the system production line queue
    machine_queue::Queue{Lawnmower}             # to keep track of lawnmower in service
    machine_status::Int64                       # the status of the blade-fitting machine, working = 0, brokendown = 1 
    n_events::Int64                             # tracks the number of events to have occur + queued
end

### Data Structure for random number generator
struct RandNumGens
    rng::StableRNGs.LehmerRNG
    interarrival_time::Function
    construction_time::Function
    interbreakdown_time::Function
    repair_time::Function
end

### Data Structure for Parameters
struct Parameters
    seed::Int64
    Time_limit::Float64
    mean_interarrival_time::Float64
    mean_construction_time::Float64
    mean_interbreakdown_time::Float64
    mean_repair_time::Float64
end

### Constructor functions
# generate a newly arrived LawnMower (where start_service, interrupt_service and end_service are unknown)
Lawnmower(id,arrival_time) = Lawnmower(id,arrival_time,Inf,Inf,0,Inf)

# Initial SystemState
function SystemState() # create an initial (empty) state
    init_time = 0.0
    init_n_lawnmowers = 0
    init_event_queue = PriorityQueue{Event,Float64}()
    init_production_line_queue = Queue{Lawnmower}()
    init_machine_queue = Queue{Lawnmower}()
    init_machine_status = 0
    init_n_events = 0
    
    return SystemState(init_time,
                       init_n_lawnmowers,
                       init_event_queue,
                       init_production_line_queue,
                       init_machine_queue,
                       init_machine_status,
                       init_n_events)
end

# constructor function for random number generator
function RandNumGens(Params::Parameters)
    rng = StableRNG( Params.seed ) # create a new RNG with seed set to that required
    interarrival_time() = rand( rng, Exponential( Params.mean_interarrival_time ) )  
    construction_time() = Params.mean_construction_time
    interbreakdown_time() = rand( rng, Exponential( Params.mean_interbreakdown_time ) )  
    repair_time() = rand( rng, Exponential( Params.mean_repair_time ) )
    
    return RandNumGens(rng,
                       interarrival_time,
                       construction_time,
                       interbreakdown_time,
                       repair_time)
end

# initialisation function for the simulation
function initialise( Params::Parameters )
    rngs = RandNumGens( Params )  # creates random number generator object
    system = SystemState()        # createthe initial stae structure  
    
    t0 = 0.0                      # add an arrival at time 0.0
    system.n_events +=1
    enqueue!( system.event_queue, OrderArrival(0,t0),t0)
           
    t1 = 150.0                    # schedule a breakdown at time 150.0
    system.n_events +=1
    enqueue!( system.event_queue, MachineBreakdown(system.n_events, t1), t1)    

    return (system, rngs)
end

### Event Handling functions - Update based on Event
#helper function for condition on machine status
machine_available(system) = (system.machine_status == 0) && (isempty(system.machine_queue))  # returns true if blade-fitting machine is working and available

#helper function for processing the next arrival
function move_order_to_machine!( system::SystemState, rngs::RandNumGens )
    # move the lawnmower order from the production line queue to being in service by the blade-fitting machine and update it
    order = dequeue!(system.production_line_queue)   # remove order from queue
    order.start_service_time = system.time           # start service 'now', record time to lawnmower
    enqueue!(system.machine_queue, order)            # put the order in construction
    
    completion_time = system.time + rngs.construction_time()     # determine the completion time of lawnmower assuming no breakdown
    
    # create an OrderComplete event for this Lawnmower and add to event queue
    system.n_events += 1
    ordercomplete_event = OrderComplete( system.n_events, completion_time)
    enqueue!(system.event_queue, ordercomplete_event, completion_time)
    return nothing
end

#General wrapper function 
function update!( system::SystemState, rngs::RandNumGens, event::Event )
    throw( DomainError("invalid event type" ) )
end

#Arrival event handler update function
function update!( system::SystemState, rngs::RandNumGens, event::OrderArrival )
    # create an arriving lawnmower order and add it to the queue
    system.n_lawnmowers += 1                               # count the new lawnmower order entering the system
    new_order = Lawnmower(system.n_lawnmowers, event.time) # new lawnmower entity
    enqueue!(system.production_line_queue, new_order)      # add the lawnmower into the production line queue    
    
    # generate next arrival and add it to the event queue
    future_arrival = OrderArrival(system.n_events, system.time + rngs.interarrival_time())
    enqueue!(system.event_queue, future_arrival, future_arrival.time)
    
    # if blade-fitting machine is working, the lawnmower order goes straight to service
    if machine_available( system ) 
        
        move_order_to_machine!( system, rngs )
    end
    return nothing
end

#Departure event handler update function
function update!( system::SystemState, rngs::RandNumGens, event::OrderComplete )
    completed_order = dequeue!(system.machine_queue)  # remove completed Lawnmower from machine
    
    if !isempty(system.production_line_queue) # if Lawnmower order is waiting, move them to blade-fitting machine
        move_order_to_machine!( system, rngs )
        
    end
    # return the completed lawnmower entity when it is leaving the system for good
    completed_order.completion_time = system.time
    return completed_order
end

#Breakdown event handler update function
function update!( system::SystemState, rngs::RandNumGens, event::MachineBreakdown )
    system.machine_status = 1                             # the machine status is indicated as broken down
    repair_duration = rngs.repair_time()                  # the repair time for the machine
    resume_time = system.time + repair_duration           # Determine the resume time of the machine   
    
    # generate a machine resume event and add it to the event queue
    machine_resumes_event = MachineResume(system.n_events, resume_time)
    enqueue!(system.event_queue, machine_resumes_event, machine_resumes_event.time)
    
    if !isempty(system.machine_queue)                                 # If the machine breaks down while in service.
        interrupted_order = dequeue!(system.machine_queue)            # get the interrupted lawnmower order
        interrupted_order.interrupted = 1                             # indicate that the lawnmower order is interrupted in its construction
        interrupted_order.interruption_duration = repair_duration     # record the duration the lawnmower comstruction was interrupted
        enqueue!(system.machine_queue,interrupted_order)              # put the interrupted lawnmower order back in the machine queue
        
        # update the corresponding ordercomplete time by including the machine repair time
        for item in keys(system.event_queue)                          # loop through events in event queue
            if typeof(item) == OrderComplete                          # check if event is of type OrderComplete (Departure)
                system.event_queue[item] += repair_duration           # update the priority of the event in the priorityqueue 
                item.time += repair_duration                          # update the time attribute of the event structure in the priorityqueue
            end
        end
    end      
    return nothing
end

#Machine Resume event handler update function
function update!( system::SystemState, rngs::RandNumGens, event::MachineResume )
    system.machine_status = 0                                   # the machine status is indicated as working
    breakdown_time = system.time + rngs.interbreakdown_time()   # determine the next machine breakdown time.
    
    # generate the next machine breakdown event and add it to the event queue
    future_breakdown = MachineBreakdown(system.n_events, breakdown_time)
    enqueue!(system.event_queue, future_breakdown, future_breakdown.time)
    
    #Given that the machine was broken, If the machine is empty and Lawnmower order is waiting, move them to blade-fitting machine upon repair 
    if isempty(system.machine_queue) && !isempty(system.production_line_queue)
        move_order_to_machine!( system, rngs )
    end
    return nothing
end

### Utility functions - writing output and reading parameter input
# function to writeout parameters
function write_parameters( output::IO, Params::Parameters ) 
    T = typeof(Params)
    for name in fieldnames(T)
        println( output, "# parameter $name = $(getfield(Params,name))" )
    end
end

 # function to writeout extra metadata
function write_metadata( output::IO )
    (path, prog) = splitdir( @__FILE__ )
    println( output, "# file created by code in $(prog)" )
    t = now()
    println( output, "# file created on $(Dates.format(t, "yyyy-mm-dd at HH:MM:SS"))" )
end

# function to write out data headings from entity attributes
function write_entity_header( entity_file::IO ) 
    println(entity_file,"id,arrival_time,start_service_time,completion_time,interrupted,interruption_duration")
end

  # function to write out headings state and events data
function write_state_header(event_file::IO)  
    println(event_file,"time,event_id,event_type,length_event_list,length_queue,in_service,machine_status")
end
       
# write to output on state of system
function write_state( event_file::IO, system::SystemState, event::Event)
    type_of_event = typeof(event)
    length_event_list = length(system.event_queue)
    length_production_line_queue = length(system.production_line_queue)
    length_machine_queue = length(system.machine_queue)
    println(event_file,"$(system.time),$(event.id),$(type_of_event),$(length_event_list),$(length_production_line_queue),$(length_machine_queue), $(system.machine_status)")
end

# write to output on completed Lawmower entity
function write_entity( entity_file::IO, entity::Lawnmower)
    println( entity_file, "$(entity.id),$(entity.arrival_time),$(entity.start_service_time),$(entity.completion_time),$(entity.interrupted),          $(entity.interruption_duration)" )
end

# A function to contruct the output file names
function output_file(seed::Int64, parameter::DataFrameRow, output::Bool)
    # file directory and name; * concatenates strings.
    dir = pwd()*"/data/factory_simulation/seed"*string(seed)*"/BDtime"*string(parameter.mean_interbreakdown_time) # directory name
    mkpath(dir)                          # this creates the directory 
    file_entities = dir*"/entities.csv"  # the name of the data file 
    file_state = dir*"/state.csv"        # the name of the data file 
    return (file_entities,file_state)
end

# A function to write the output data of the simulation to the files    
function write_output_file(file_entities::String,file_state::String,Params::Parameters) 
    fid_entities = open(file_entities, "w") # open the file for writing
    fid_state = open(file_state, "w")       # open the file for writing
    
    # loop through the output files and write the metadata and parameters to set up the files
    for fid in [fid_entities,fid_state]
        write_metadata( fid )
        write_parameters( fid, Params )
    end
    
    #write out the headers to the output files
    write_entity_header(fid_entities)
    write_state_header(fid_state)
    
    return (fid_entities,fid_state)
end

#A function to read the input parameter csv file
function read_parameters(file_parameter::String)
    parameter_set = CSV.read(file_parameter, DataFrame, comment="#" )
    return parameter_set    
end

### function to run the main loop of the simulation
function run!(system::SystemState, rngs::RandNumGens, Time_limit::Float64,output::Bool, fid_state::IO, fid_entities::IO)
    # main simulation loop
    while system.time < Time_limit
        # grab the next event from the event queue
        (event,time) = dequeue_pair!(system.event_queue)
        system.time = time  # advance system time to that of the next event
        system.n_events += 1      # increase the event counter
        
        # write out event and state data before event
        if output
            write_state( fid_state, system, event)
        end
        
        # update the system based on the next event, and spawn new events. 
        completed_order = update!( system, rngs, event )
       
        # write out entity data if it was a completed order from the system
        if completed_order !== nothing && output
            write_entity( fid_entities, completed_order )
        end
    end
end

### main worker function to run the simulation
function run_factory_simulation(seed::Int64, Time_limit::Float64, parameter::DataFrameRow, output::Bool, file_state::String, file_entities::String)
    # define the parameter object
    Params = Parameters(seed,
                        Time_limit,
                        parameter.mean_interarrival_time,
                        parameter.mean_construction_time,
                        parameter.mean_interbreakdown_time,
                        parameter.mean_repair_time) 
    
    #Setup and write the output files
    if output
        (fid_entities,fid_state) = write_output_file(file_entities::String,file_state::String, Params::Parameters) 
    else
        (fid_entities,fid_state) = (IOBuffer(),IOBuffer())
    end
    
    #Initialisae and  run the actual simulation
    (system,rngs) = initialise( Params ) 
    run!( system, rngs, Time_limit, output, fid_state, fid_entities)

    # to close the files
    if output
        close( fid_entities )
        close( fid_state )
    end
end
    
    
        