

import matplotlib.pyplot as plt
import pandas as pd


# Read the data from the CSV file
data = pd.read_csv('./resource_monitor_data.csv')
# print(data.columns)

# List the unique resources in the data
resources = data['Resource'].unique()


# Create a new plot
fig, ax = plt.subplots()

# For each unique resource, plot a line graph
for resource in resources:
    # Filter data for the current resource
    resource_data = data[data['Resource'] == resource]
    # Plot daily use for the current resource
    ax.plot(resource_data['Day'], resource_data['Daily_Use'], label=resource)

ax.set_xlabel('Day') # Set the x-axis label
ax.set_ylabel('Daily Use') # Set the y-axis label
ax.set_title('Daily Use of Resources Over Time') # Set the plot title

# ax.legend()

fig