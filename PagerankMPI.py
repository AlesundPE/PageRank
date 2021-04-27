from mpi4py import MPI
import sys
#import splitInput
#import math
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
threadhold = 0.5
## To do large input split with memory usage comtrol
#MemUsage_Limit = 10 # At maximum, the program use 10 Kb memory
#def mapPhase(path):
#    total_size = os.path.getsize(path)
#    if total_size > MemUsage_Limit*1000:
#        num_split = min(size, math.ceil(total_size/MemUsage_Limit/1000))
#        inputSplit(path, num_split, MemUsage_Limit)
#    f = open("links", "r")
#    for lines in f:
#        source, dests = lines.split()
#        dests = dests.split(",")
data = []

def customHash(source):
    index = source % size
    return index

def areTrue(cond1, cond2):
    return cond1 == True and cond2 == True

def isConverged(old, new, globalTracker):
    result = True
    for i, old_value in enumerate(old, 0):
        if abs(new[i] - old_value) > threadhold:
            result = False
            break
        else:
            result = True
    comm.Barrier()
    if rank == 0:
        for i, dest in enumerate(globalTracker, 0):
            if dest == 0:
                continue
            cond2 = comm.recv(source = dest, tag = 99)
            result = areTrue(result, cond2)
    else:
        comm.send(result, dest = 0, tag = 99)
    result = comm.bcast(result, root = 0)
    return result

def isConverged2(old, new, globalTracker):
    return True

# following code seperates input into workers
if len(sys.argv) == 2:
    start = 0
    end = 0
    globalTracker = []
    if rank == 0:
        path = sys.argv[1]
        data = [[] for i in range(size)]
        with open(path, "r") as f:
            for line in f:
                source, dests = line.split()
                dests = dests.split(",")
                dests = list(map(lambda x : int(x), dests))
                dests.append(int(source))
                index = customHash(int(source))
                if index not in globalTracker:
                    globalTracker.append(index) # globalTracker tracks which worker will participate in pagerank calculation
                data[index].append(dests)
        for i, _ in enumerate(data, 1):
            if i == size:
                break
            comm.send(data[i], dest = i, tag = 0)
            comm.send(globalTracker, dest = i, tag = 0)
        data = data[0]
        #print("{} from {}".format(data, rank))
    else:
        data = comm.recv(source = 0, tag = 0)
        globalTracker = comm.recv(source = 0, tag = 0)
        #print("{} from {}".format(data, rank))
    #print("globalTracker is {} from rank {}".format(globalTracker, rank))
    comm.Barrier()
    # initialize local variables

    dests = [] # records corresponding dests nodes
    sources = [] # all source nodes that each work handles
    for row in data:
        sources.append(row[-1])
        if len(row) == 1:
            dests.append([])
        else:
            dests.append(row[:len(row)-1])
    local_weights = [1.0]*len(sources) # each worker responsible len(data) sources, and the weight of each source is setted to 1
    iteration = 0
    contribution = []
    # Start iteration
    if data is not []:
        while True:
            if rank == 0:
                # Start Timeing
                start = MPI.Wtime()
            contribution.clear()
            # Calculate contribution to neighbors for each source
            for i, weight in enumerate(local_weights, 0):
                contribution.append([dests[i], weight/len(dests[i])])
            global_contribution = []
            for i in globalTracker:
                result = comm.bcast(contribution, root = i)
                global_contribution.append(result)
            #print("global_contribution is {} from rank {}".format(global_contribution, rank))
            #print("local_weights is {} from rank {}".format(contribution, rank))
            contribution = [0.0]*len(sources)
            for row in global_contribution:
                for col in row:
                    for i, source in enumerate(sources, 0):
                        if source in col[0]:
                            contribution[i] += col[1]
            contribution = list(map(lambda x : 0.15 + 0.85*x, contribution))
            iteration += 1
            if rank == 0:
                # End Timeing
                end = MPI.Wtime()
                print("iteration {} takes {} seconds".format(iteration - 1, end-start))
            comm.Barrier()
            if isConverged(local_weights, contribution, globalTracker):
                local_weights = contribution.copy()
                print("Sources are {} from rank {}".format(sources, rank))
                print("contribution are {} from rank {} at iteration {}".format(contribution, rank, iteration))
                break
            else:
                local_weights = contribution.copy()
else:
    if rank == 0:
        print("Please Read the usage in the Readme file.")
