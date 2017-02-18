import sys
import os
import importlib
import argparse
import prov.model

parser = argparse.ArgumentParser()
parser.add_argument("contributor_folder")
args = parser.parse_args()

# Extract the algorithm classes from the modules in the
# subdirectory specified on the command line.
path = args.contributor_folder
algorithms = []
for r,d,f in os.walk(path):
    for file in f:
        if file.split(".")[-1] == "py":
            name_module = ".".join(file.split(".")[0:-1])
            module = importlib.import_module(path + "." + name_module)
            algorithms.append(module.__dict__[name_module])

# Create an ordering of the algorithms based on the data
# sets that they read and write.
datasets = set()
ordered = []
while len(algorithms) > 0:
    for i in range(0,len(algorithms)):
        if set(algorithms[i].reads).issubset(datasets):
            datasets = datasets | set(algorithms[i].writes)
            ordered.append(algorithms[i])
            del algorithms[i]
            break

# Execute the algorithms in order.
provenance = prov.model.ProvDocument()
for algorithm in ordered:
    algorithm.execute()
    provenance = algorithm.provenance(provenance)

# Display a provenance record of the overall execution process.
print(provenance.get_provn())

## eof