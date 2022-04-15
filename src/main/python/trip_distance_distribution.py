from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import halfnorm

df = pd.read_csv('../../../output/distances.csv', delim_whitespace=True)
print(df.head())
minimum = min(df['Distance'])
maximum = max(df['Distance'])
print(np.array(df['Distance']))

# https://stackoverflow.com/questions/26218704/matplotlib-histogram-with-collection-bin-for-high-values
fig, ax = plt.subplots()
N_bins = 21
bins = np.arange(0, N_bins)
_, bins, patches = plt.hist(np.clip(df['Distance'], bins[0], bins[-1]), density=True, bins=bins, color='cornflowerblue',
                            label='Proportion of trips with this length')

# sns.distplot(np.clip(df['Distance'], bins[0], bins[-1]), bins=bins, label='Proportion of trips with this length')

x_labels = bins[1:].astype(str)
x_labels[-1] += '+'

N_labels = len(x_labels)
plt.xlim([0, N_bins-1])
plt.xticks(np.arange(N_labels) + 0.5)
ax.set_xticklabels(x_labels)
plt.setp(patches, linewidth=0)

# mu, std = halfnorm.fit(df['Distance'].values)
mu, std = -2.4926370108626827e-09, 46.40674593036614
x_min, x_max = 0, N_bins-1
x = np.linspace(x_min, x_max, 100)
p = halfnorm.pdf(x, mu, std)
plt.plot(x, p, 'k', linewidth=2)

plt.title('Trip length distribution')
plt.xlabel('Trip length (km)')
plt.ylabel('Proportion of all trips')
plt.legend()
fig.tight_layout()
# df.plot()
plt.show()
