import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import preprocessing

originalArray = np.genfromtxt('part-r-00000.csv', delimiter=',', dtype='unicode')

smallerArray = originalArray[:, 1:]

scaler = preprocessing.MinMaxScaler()

scaler.fit(smallerArray)

preprocessing.MinMaxScaler(copy=True, feature_range=(0, 1))

scaledData = scaler.transform(smallerArray)

print (scaledData)

target = originalArray[:, 0]
# print("Target : ", target)

df_scaled = pd.DataFrame(scaledData)

print(df_scaled)

df_scaled.index = target
df_scaled.columns = ['Solar Radiation', 'Wind Speed']

print(df_scaled)

df_scaled.plot.line()
plt.tight_layout()
plt.show()

# fig, ax = plt.subplots()
#
# df_scaled.plot(x=df_scaled.index, y='Solar Radiation', ax=ax)
#
# df_scaled.plot(x=df_scaled.index, y='Wind Speed', ax=ax, secondary_y=True)