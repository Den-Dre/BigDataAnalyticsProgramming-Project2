import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv("~/Desktop/part-r-00000", delim_whitespace=True, parse_dates=True, names=["Date", "Revenue"])
df.plot(kind="bar", x='Date', y='Revenue')
# plt.xticks(rotation=45, ha='right')
plt.show()
