from utils.io_utils import load_json
import matplotlib.pyplot as plt

path = '/home/cc/nexus_correlation_discovery/evaluation/run_time/chicago_1m/100_st_schemas/cnt_ratio.json'

data = load_json(path)

# data.sort(key=lambda x: x[1])

v_cnts = [x[0] for x in data]
print(len(v_cnts))
row_to_read = [x[1] for x in data]
join_cost = [x[3] for x in data]
join_time = [x[4] for x in data]
find_join_time = [x[2] for x in data]
ratios = [(x[2]/(x[0]+x[1]))/(x[4]/x[3]) for x in data]

r = []
for x in data:
    ratio = (x[2]/(x[0]+x[1]))/(x[4]/x[3])
    if x[0] > 100000:
        r.append(ratio)

import numpy as np
print(np.mean(r), np.median(r))

# Create a new figure and axes
fig, ax = plt.subplots()

# Plot y versus x as lines and/or markers
ax.scatter(v_cnts, ratios)
# ax.set_xlim(0, 4)
# Set labels for x and y axes
ax.set_xlabel('row_to_read')
ax.set_ylabel('find_join_time')

# Set title for the plot
ax.set_title('Relationship between x and y')

# Display the plot
plt.show()
plt.savefig('plot.png')