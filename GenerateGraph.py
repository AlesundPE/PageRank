from random import randint
sources = 60000 # Control the total number of webpages
max_links = 30 # Control the maximum number of links that each page can have
with open("links2", "w") as f: # you can change the name of output file here. Eg. change links2 to links3
    for i in range(sources):
        dests = ""
        num_links = randint(1,max(max_links,1))
        for _ in range(num_links):
            dest = i
            while dest == i:
                dest = randint(1, sources)
            dests += str(dest) + ","
        dests = dests[:-1]
        if i != sources - 1:
            result = "{} {}\n".format(i, dests)
        elif i == sources - 1:
            result = "{} {}".format(i, dests)
        f.write(result)