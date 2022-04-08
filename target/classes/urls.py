import webbrowser
import pandas as pd
from matplotlib import pyplot as plt

if __name__ == '__main__':
    coordinates = "37.79536,-122.40651 37.79679,-122.40819 37.79536,-122.41209 37.79501,-122.41518 37.79463,-122.41778 37.79409,-122.41972 37.7941,-122.41975 37.78841,-122.40861 37.79175,-122.40932 37.79547,-122.40992 37.79987,-122.40594 37.79699,-122.40362 37.79334,-122.40284 37.79013,-122.4032 37.79013,-122.40322 37.79001,-122.40338 37.80313,-122.42576 37.7996,-122.4243 37.79263,-122.42286 37.78464,-122.42124 37.77997,-122.42029 37.77404,-122.419 37.76997,-122.41776 37.76947,-122.41488 37.76168,-122.40616 37.74601,-122.40491 37.73002,-122.40398 37.71375,-122.39705 37.69591,-122.39227 37.67871,-122.38842 37.66361,-122.39936 37.64848,-122.40679 37.63273,-122.40355 37.61753,-122.39935 37.61523,-122.38826 37.61762,-122.38515 37.61802,-122.38625 37.61689,-122.38799 37.61553,-122.39774 37.63185,-122.40261 37.64833,-122.40644 37.66288,-122.40038 37.67628,-122.38857 37.6927,-122.39136 37.70943,-122.39512 37.72549,-122.40181 37.74085,-122.40783 37.75607,-122.40318 37.75975,-122.4063"

    # with open("../java/incorrect") as f:
    #     lines = f.readlines()
    #     for line in lines:
    #         parts = line.replace(" ", "").split(",")
    #         try:
    #             url = "https://www.google.com/maps/dir/?api=1&origin=" + parts[2] + '%2C' + parts[3] + "&destination=" + \
    #                   parts[6] + '%2C' + parts[7]
    #             print(url)
    #         except IndexError:
    #             print("Error")

    # parts = coordinates.replace(",", "%2C").split(" ")
    # url = "https://www.google.com/maps/dir/?api=1&origin=" + parts[0] + "&destination=" + parts[-1] + \
    #       "&waypoints="
    # for i in range(len(parts) - 1):
    #     # webbrowser.open("https://www.google.com/maps/dir/?api=1&origin=" + parts[i] + "&destination=" + parts[i+1], new=0)
    #     print("https://www.google.com/maps/dir/?api=1&origin=" + parts[i] + "&destination=" + parts[i+1])
    #
# url += "%7C".join(parts[1:-1])
# print(url)
