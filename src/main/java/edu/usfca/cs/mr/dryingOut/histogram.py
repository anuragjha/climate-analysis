import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
# from matplotlib.ticker import PercentFormatter

# Fixing random state for reproducibility
np.random.seed(19680801)


N_points = 100000
n_bins = 20

print os.getcwd()
#
list = []
#
f=open("../../../../../../../../outputDOJ/part-r-00000", "r")

if f.mode == 'r':
    fl =f.readlines()
    for line in fl:
        list.append(line.split()[1])
        print(line)


# Generate a normal distribution, center at x=0 and y=5
x = np.random.randn(N_points)
#y = .4 * x + np.random.randn(100000) + 5
y = list

fig, axs = plt.subplots(1, 2, sharey=True, tight_layout=True)

# We can set the number of bins with the `bins` kwarg
axs[0].hist(x, bins=n_bins)
axs[1].hist(y, bins=n_bins)

plt.show()