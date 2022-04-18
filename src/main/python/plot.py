import statistics

import pandas as pd
from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np
import scipy.stats as st
from fitter import Fitter
import pylab
import seaborn as sns


# df = pd.read_csv("~/Desktop/revenue per month wo checking subsequent segments", delim_whitespace=True, parse_dates=True, names=["Date", "Revenue"])
# df.plot(kind="bar", x='Date', y='Revenue')
# # plt.xticks(rotation=45, ha='right')
# plt.show()

def get_seconds(string):
    parts = string[:-1].split('m')
    return float(parts[0]) * 60 + float(parts[1])


def plot_reduce_execution_times(ax, x=None, color='blue'):
    # Without the in sea check:
    old_reduce_execution_times = {
        9: "9m17.379s",
        11: "9m53.635s",
        13: "9m26.603s",
        15: "9m24.209s",
        17: "9m22.817s",
    }

    # originally: 9, 11, 13, 15, 17
    reduce_execution_times = {
        5: '11m40.417s',
        6: '11m44.482s',
        7: '11m14.024s',
        8: '10m35.168s',
        # 9: '10m49.846s',
        10: '11m32.824s',
        11: '11m27.221s',
        12: '11m30.318s',
        13: '10m49.362s',
        14: '11m4.984s',
        15: '10m48.254s',
        16: '10m52.068s',
        17: '10m41.018s',
        18: '10m57.259s',
        19: '10m34.019s',
        20: '10m44.257s',
        21: '10m51.077s',
        22: '10m47.312s',
        23: '10m54.167s',
        24: '11m43.781s',
        25: '11m0.632s',
        26: '10m45.967s',
        28: '14m33.529s',
        29: '16m44.189s',
        30: '36m31.182s',
        31: '41m8.241s',
        32: '18m33.470s',
        33: '12m13.283s',
        34: '11m59.390s',
        35: '11m57.720s',
    }

    custom_reduce_execution_times = {
        5:	'11m15s',
        6:	'11m17s',
        7:	'10m44s',
        8:	'10m9s',
        10:	'11m5s',
        11:	'10m56s',
        12:	'11m2s',
        13:	'10m26s',
        14:	'10m39s',
        15:	'10m27s',
        16:	'10m27s',
        17:	'10m20s',
        18:	'10m33s',
        19:	'10m10s',
        20:	'10m21s',
        21:	'10m26s',
        22:	'10m16s',
        23:	'10m32s',
        24:	'10m19s',
        25:	'10m33s',
        26:	'10m20s',
        28:	'12m52s',
        29:	'12m48s',
        30:	'13m48s',
        31:	'13m30s',
        32:	'12m42s',
        33:	'11m49s',
        34:	'11m40s',
        35:	'11m40s',
    }

    for key, time in custom_reduce_execution_times.items():
        custom_reduce_execution_times[key] = get_seconds(time) / 60
    red_ordered_times = dict(sorted(custom_reduce_execution_times.items()))
    # red_ordered_times.pop(9)
    # ax.set_title('Execution time in function of number of reducers')
    ax.set_xlabel('Number of reducers')
    ax.set_ylabel('Execution time (minutes)', color=color)
    ax.tick_params(axis='y', color=color)
    # print(min(red_ordered_times, key=red_ordered_times.get))
    if x is None:
        x = red_ordered_times.keys()
    ax.plot(x, red_ordered_times.values(),  # list(x)[:30 - 10], list(red_ordered_times.values())[:30 - 10],
            # markevery=[min(red_ordered_times, key=red_ordered_times.get)], marker="o",
            # markevery=[list(red_ordered_times.values()).index(min(red_ordered_times.values()))], marker="o",
            markerfacecolor='green', label='Execution time', color=color)


def plot_map_execution_times(ax, x=None, color='blue'):
    map_execution_times = {
        1073741824: '12m20.095s',
        134217728: '12m32.617s',
        16777216: '38m4.481s',
        2147483648: '12m32.329s',
        268435456: '12m4.005s',
        33554432: '24m8.898s',
        536870912: '11m52.856s',
        67108864: '15m12.675s',
        8388608: '70m29.642s'
    }
    map_execution_times = {
        8388608: '70m29.642s',
        16777216: '38m4.481s',
        33554432: '24m8.898s',
        67108864: '15m12.675s',
        134217728: '12m32.617s',
        268435456: '23m34.919s',
        536870912: '19m49.557s',
        1073741824: '09m8.805s',
        2147483648: '11m36.873s',
        4294967296: '12m14.186s',
        8589934592: '13m41.728s',
        17179869184: '23m29.812s',
    }

    custom_execution_times = {
        # 131072:	'8m49s',
        # 262144:	'8m39s',
        # 524288:	'8m38s',
        # 1048576:	'8m45s',
        # 2097152:	'8m41s',
        4194304:	'8m37s',
        8388608:	'8m41s',
        16777216:	'8m35s',
        33554432:	'8m46s',
        67108864:	'8m39s',
        134217728:	'8m38s',
        268435456:	'7m30s',
        536870912:	'6m51s',
        1073741824:	'9m3s',
        2147483648:	'9m10s',
        4294967296:	'9m3s',
        8589934592:	'9m17s',
        17179869184: '9m52s',
    }

    new_map_execution_times = {}
    for key, time in custom_execution_times.items():
        new_key = int(float(key) / (1024 * 1024))
        time_seconds = get_seconds(time)
        new_map_execution_times[new_key] = time_seconds / 60
    map_ordered_times = dict(sorted(new_map_execution_times.items()))
    print(map_ordered_times)

    # ax.set_title('Execution time in function of number of reducers')
    ax.set_xlabel('Split size (Mb)')
    ax.set_ylabel('Execution time (minutes)', color=color)
    # plt.xticks(list(map_ordered_times.keys()), rotation=30, ha='right')
    # plt.xticks(np.arange(0, max(map_ordered_times.keys()), 256))
    if x is None:
        x = map_ordered_times.keys()
    ax.semilogx(x, map_ordered_times.values(),
                # markevery=[19],
                # marker="o", markerfacecolor='red',
                label='Execution time', color=color)
    x_ticks = list(map_ordered_times.keys())
    # x_ticks.remove(16)
    # x_ticks.remove(8)
    # x_ticks.remove(64)
    # ax.set_xticks(x_ticks)
    # ax.legend()


def plot_trip_revenue_per_month(ax):
    df = pd.read_csv('./revenuePerMonthOutput', delim_whitespace=True, names=['Date', 'Revenue'], parse_dates=True)
    print('Total revenue: ' + str(sum(df['Revenue'])))
    ax.set_title('Revenue per month')
    ax.set_xlabel('Date (per month)')
    ax.set_ylabel('Revenue (in million dollars)')
    plt.xticks(rotation=90)
    short_dates = ['\'' + date[2:] for date in df['Date']]  # Short notation for the years
    ax.bar(short_dates, df['Revenue'])
    # df.plot(x='Date', y='Revenue', kind='bar')


# https://stackoverflow.com/questions/37487830/how-to-find-probability-distribution-and-parameters-for-real-data-python-3
def get_best_distribution(data):
    dist_names = ["norm", "exponweib", "weibull_max", "weibull_min", "pareto", "genextreme"]
    dist_results = []
    params = {}
    for dist_name in dist_names:
        dist = getattr(st, dist_name)
        param = dist.fit(data)

        params[dist_name] = param
        # Applying the Kolmogorov-Smirnov test
        D, p = st.kstest(data, dist_name, args=param)
        print("p value for " + dist_name + " = " + str(p))
        dist_results.append((dist_name, p))

    # select the best fitted distribution
    best_dist, best_p = (max(dist_results, key=lambda item: item[1]))
    # store the name of the best fit and its p value

    print("Best fitting distribution: " + str(best_dist))
    print("Best p value: " + str(best_p))
    print("Parameters for the best fit: " + str(params[best_dist]))

    return best_dist, best_p, params[best_dist]


def plot_peak_memory(ax, x, color='blue'):
    nbreducers_map_physical_memory = [
        635826176,
        661565440,
        649564160,
        647663616,
        656801792,
        625053696,
        625098752,
        661516288,
        638812160,
        658898944,
        621871104,
        656490496,
        620535808,
        648978432,
        639549440,
        627527680,
        669605888,
        617541632,
        620052480,
        619814912,
        633606144,
        663101440,
        618356736,
        658190336,
        645718016,
        656510976,
        667058176,
        626024448,
        619864064,
    ]
    nbreducers_map_virtual_memory = [
        2583412736,
        2583535616,
        2597978112,
        2577551360,
        2579705856,
        2585468928,
        2580922368,
        2569228288,
        2581942272,
        2586394624,
        2587344896,
        2581094400,
        2600247296,
        2592141312,
        2582962176,
        2592907264,
        2589569024,
        2575978496,
        2568978432,
        2587082752,
        2588880896,
        2587103232,
        2599088128,
        2589962240,
        2587029504,
        2587373568,
        2586353664,
        2585845760,
        2568224768,
    ]
    nbreducers_reduce_physical_memory = [
        1013268480,
        1009426432,
        1016758272,
        985100288,
        1016799232,
        1020211200,
        1007874048,
        1021317120,
        1008504832,
        1006321664,
        1027645440,
        1016573952,
        1013944320,
        1013424128,
        994918400,
        1031622656,
        1022005248,
        997928960,
        1011183616,
        1011904512,
        1012305920,
        999833600,
        982446080,
        1008816128,
        1008746496,
        977969152,
        999407616,
        980267008,
        1008627712,
    ]
    nbreducers_reduce_virtual_memory = [
        2577117184,
        2593701888,
        2584203264,
        2586853376,
        2584940544,
        2587099136,
        2578919424,
        2588483584,
        2586927104,
        2588459008,
        2578911232,
        2581901312,
        2579791872,
        2581401600,
        2585714688,
        2588717056,
        2592800768,
        2588946432,
        2591866880,
        2589999104,
        2589347840,
        2576207872,
        2592296960,
        2580226048,
        2577010688,
        2581716992,
        2585366528,
        2577854464,
        2584690688,
    ]

    nbmappers_map_physical_memory = [
        670699520,
        650543104,
        687214592,
        680013824,
        616615936,
        651907072,
        617185280,
        647884800,
        633999360,
        623022080,
        623144960,
        650866688,
        623611904,
    ]
    nbmappers_map_virtual_memory = [
        2596212736,
        2563915776,
        2608807936,
        2598154240,
        2563682304,
        2568544256,
        2568192000,
        2607198208,
        2590703616,
        2583040000,
        2585755648,
        2578366464,
        2567823360,
    ]
    nbmappers_reduce_physical_memory = [
        1025884160,
        1050193920,
        1022808064,
        1017921536,
        1033015296,
        1002848256,
        1013145600,
        1016549376,
        1015635968,
        417030144,
        417042432,
        422191104,
    ]
    nbmappers_reduce_virtual_memory = [
        2588102656,
        2600923136,
        2587738112,
        2582147072,
        2580647936,
        2579935232,
        2580070400,
        2579439616,
        2583752704,
        2580320256,
        2579001344,
        2582777856,
    ]

    data = nbmappers_map_physical_memory
    ax.set_ylabel('Peak Map physical memory usage (bytes)', color=color)
    ax.tick_params(axis='y', color=color)
    ax.plot(x, data, label='Peak Reduce physical memory usage (bytes)', color=color,
            # markevery=[data.index(min(data))]
            )
    # ax.set_xticks(list(plt.xticks()[0]) + [8, 19, 29])
    # ax.set_title('Peak Map virtual memory usage in function of number of reducers')
    # ax.set_xlabel('Number of reducers')
    # ax.set_ylabel('Peak Map virtual memory usage (bytes)')


def plot_two_axes(ax):
    # nb_reduces_xs = [i for i in range(5, 36) if i not in [9, 27]]
    splitSizes_xs = [
        4194304,
        8388608,
        16777216,
        33554432,
        67108864,
        134217728,
        268435456,
        536870912,
        1073741824,
        2147483648,
        4294967296,
        8589934592,
        17179869184,
    ]
    splitSizes_xs = [x / 1024**2 for x in splitSizes_xs]
    # xs = np.delete(xs, 9 - 5)
    plot_map_execution_times(ax, splitSizes_xs)
    ax2 = ax.twinx()
    plot_peak_memory(ax2, splitSizes_xs, 'red')


def fit_trip_lengths():
    data = pd.read_csv('../../../output/distances.csv', delim_whitespace=True, header=0)['Distance']
    N_bins = 21
    bins = np.arange(0, N_bins)
    # data = [x for x in data if x < 25]
    # ['halfnorm', 'anglit', 'cosine', 'rayleigh', 'norm']
    f = Fitter(np.clip(data, bins[0], bins[-1]), distributions=['foldcauchy'], bins=bins, density=True)
    f.fit()
    print(f.summary())
    pylab.title('Distribution of trip lengths')
    pylab.xlabel('Trip length (km)')
    pylab.ylabel('Proportion of trips with this length')
    pylab.legend(['Folded Cauchy PDF'])
    pylab.grid(False)

    x_labels = bins[1:].astype(str)
    x_labels[-1] += '+'

    N_labels = len(x_labels)
    plt.xlim([0, N_bins-1])
    plt.xticks(np.arange(N_labels) + 0.5)
    ax = pylab.gca()
    ax.set_xticklabels(x_labels)
    # plt.setp(patches, linewidth=0)
    print(f.get_best(method='sumsquare_error'))


if __name__ == '__main__':
    mpl.rcParams['savefig.directory'] = '/home/andreas/Documents/KUL/1e master/Big Data Analytics ' \
                                        'Programming/Project2/plots'
    # df = pd.read_csv('../../../output/distances.csv', delim_whitespace=True, header=0)
    fig, ax1 = plt.subplots()
    # plot_reduce_execution_times(ax1, None, 'blue')  # works
    # plot_map_execution_times(ax1)
    plot_two_axes(ax1)  # works
    # plot_trip_revenue_per_month(ax1)
    plt.title('Map execution time & memory usage as a function of splitSize')
    plt.show()
