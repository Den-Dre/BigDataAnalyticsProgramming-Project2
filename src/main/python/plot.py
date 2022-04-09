import pandas as pd
from matplotlib import pyplot as plt

# df = pd.read_csv("~/Desktop/revenue per month wo checking subsequent segments", delim_whitespace=True, parse_dates=True, names=["Date", "Revenue"])
# df.plot(kind="bar", x='Date', y='Revenue')
# # plt.xticks(rotation=45, ha='right')
# plt.show()

execution_times = {
    9: "9m17.379s",
    11: "9m53.635s",
    13: "9m26.603s",
    15: "9m24.209s",
    17: "9m22.817s"
}
for key, time in execution_times.items():
    execution_times[key] = float(time[0]) * 60 + float(time[2:4]) + float(time[5:-1]) / 1000
ordered_times = dict(sorted(execution_times.items()))

plt.plot(ordered_times.keys(), ordered_times.values())
plt.show()


