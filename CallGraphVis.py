import ijson
import networkx as nx
import matplotlib.pyplot as plt

callers = set()
dict_calls = {}
with open("./data/calls.json", 'r') as data:
    for call in ijson.items(data, "item"):
        callers.add(call["caller_id"])
        temp = (call["caller_id"], call["callee_id"])
        if temp in dict_calls:
            dict_calls[temp] += 1
        else:
            dict_calls[temp] = 1

calls = []
for key, value in dict_calls.items():
    calls.append((key[0], key[1], {"weight": value}))

G = nx.DiGraph()
G.add_nodes_from(list(callers))
G.add_edges_from(calls)
nx.draw_spring(G)
plt.show()
