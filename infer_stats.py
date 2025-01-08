import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import matplotlib.pyplot as plt
import numpy as np


def read_json_directory(directory):
    dataframes = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            df = pd.read_json(file_path, lines=True)
            dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df


def check_normality(df, column):
    statistics, p_value = stats.normaltest(df[column])
    print(f"Test Statistic: {statistics}, p-value: {p_value}")
    if p_value > 0.05:
        print(f"The {column} data appears to be normally distributed.")
    else:
        print(f"The {column} data does not appear to be normally distributed.")


def check_equal_variances(df, column):

    group1 = df[df['is_popular'] == True][column]
    group2 = df[df['is_popular'] == False][column]
    statistics, p_value = stats.levene(group1, group2)
    print(f"Test Statistic: {statistics}, p-value: {p_value}")
    if p_value > 0.05:
        print(f"The variances for {column} are approximately equal.")
    else:
        print(f"The variances for {column} are not equal.")


def print_stars():
    char = '*'
    repetitions = 60
    print(char * repetitions)


def check_figures_dir():
    if not os.path.exists("figures"):
        os.makedirs("figures")

def plot_combined_histograms(df):
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))
    plt.subplot(1, 3, 1)
    sns.histplot(data=df, x='avg_tmax', hue='is_popular',
                 kde=True, palette=['skyblue', 'salmon'], bins=20)
    plt.xlabel("Average Max Temperature (°C)")
    plt.subplot(1, 3, 2)
    sns.histplot(data=df, x='avg_tmin', hue='is_popular',
                 kde=True, palette=['skyblue', 'salmon'], bins=20)
    plt.xlabel("Average Min Temperature (°C)")
    plt.subplot(1, 3, 3)
    sns.histplot(data=df, x='avg_prcp', hue='is_popular',
                 kde=True, palette=['skyblue', 'salmon'], bins=20)
    plt.xlabel("Average Precipitation (mm)")
    plt.suptitle("Histograms - Popular vs. Unpopular Cities")
    plt.tight_layout()

    check_figures_dir()

    plt.savefig(f"figures/combined_histogram.png")
    plt.close()


def do_ttest(group1, group2, column):
    statistics, p_value = stats.ttest_ind(group1[column], group2[column])
    print(f"Test Statistic: {statistics}, p-value: {p_value}")
    if p_value < 0.05:
        print(
            f"Reject the NULL hypothesis")
    else:
        print(f"Fail to reject the NULL hypothesis")
        
def plot_random_histograms(df):
    plt.figure(figsize=(10, 6))

    plt.hist(df['avg_tmax'], bins=30,
             alpha=0.7, color='r', label='Avg Tmax')
    plt.hist(df['avg_tmin'], bins=30,
             alpha=0.7, color='b', label='Avg Tmin')

    plt.xlabel('Temperature (°C)')
    plt.ylabel('Frequency')
    plt.title('Histogram of Avg Tmax and Avg Tmin in Random Cities')
    plt.legend()
    plt.grid(True)

    check_figures_dir()

    plt.savefig('figures/random_histogram.png')
    plt.close() 

def plot_hist(popular_cities_df, unpopular_cities_df):
    #fig, axs = plt.subplots(3, 2)
    fig, axs = plt.subplots(2, 3)
    #fig.suptitle('popular_cities')
    
    # plot popular cities
    #axs[0, 0].set_title('avg_tmax')
    axs[0, 0].set(ylabel='popular cities')
    axs[1, 0].set(ylabel='unpopular cities')

    axs[0, 0].hist(popular_cities_df['avg_tmax'])
    axs[1, 0].hist(unpopular_cities_df['avg_tmax'])
    axs[1, 0].set(xlabel='avg_tmax')

    axs[0, 1].hist(popular_cities_df['avg_tmin'])
    axs[1, 1].hist(unpopular_cities_df['avg_tmin'])
    axs[1, 1].set(xlabel='avg_tmin')

    axs[0, 2].hist(popular_cities_df['avg_prcp'])
    axs[1, 2].hist(unpopular_cities_df['avg_prcp'])
    axs[1, 2].set(xlabel='avg_prcp')

    plt.savefig(f"figures/infer_histograms.png", bbox_inches='tight')

def transform_to_log(df, col):
    df[col] = df[col].apply(np.log, axis=1)

def transform_to_sqrt(df, col):
    #popular_cities_df["avg_prcp"] = np.sqrt(popular_cities_df["avg_prcp"])
    df[col] = df[col].apply(np.sqrt, axis=1)

def transform_to_exp(df, col):
    #popular_cities_df["avg_prcp"] = np.sqrt(popular_cities_df["avg_prcp"])
    df[col] = df[col].apply(np.exp, axis=1)

def transform_to_pow(df, col):
    #popular_cities_df["avg_prcp"] = np.sqrt(popular_cities_df["avg_prcp"])
    #df[col] = df[col]*df[col]
    df[col] = np.power(df[col], 2)


def main(in_directory_1, in_directory_2, in_directory_3):
    popular_cities_df = read_json_directory(in_directory_1)
    popular_cities_df['is_popular'] = True
    transform_to_sqrt(popular_cities_df, "avg_prcp")

    unpopular_cities_df = read_json_directory(in_directory_2)
    unpopular_cities_df['is_popular'] = False
    transform_to_log(unpopular_cities_df, "avg_prcp")

    frames = [popular_cities_df, unpopular_cities_df]
    combined_cities_df = pd.concat(frames)
    transform_to_sqrt(combined_cities_df, "avg_prcp")

    plot_hist(popular_cities_df, unpopular_cities_df)

    random_cities_df = read_json_directory(in_directory_3)

    print("                  *** Inferential Stats ***")
    print("                  Before transformation")
    print("\n                  Test for Normal Distribution: \n")

    print("       Popular Cities Data: ")

    check_normality(popular_cities_df, 'avg_tmax')
    check_normality(popular_cities_df, 'avg_tmin')
    check_normality(popular_cities_df, 'avg_prcp')

    print("\n       Unpopular Cities Data: ")

    check_normality(unpopular_cities_df, 'avg_tmax')
    check_normality(unpopular_cities_df, 'avg_tmin')
    check_normality(unpopular_cities_df, 'avg_prcp')
    
    print("\n      For Combined Cities DataFrame (Popular & Unpopular):\n")

    check_normality(combined_cities_df, 'avg_tmax')
    check_normality(combined_cities_df, 'avg_tmin')
    check_normality(combined_cities_df, 'avg_prcp')

    print("\n      For Random Cities DataFrame:\n")

    check_normality(random_cities_df, 'avg_tmax')
    check_normality(random_cities_df, 'avg_tmin')
    check_normality(random_cities_df, 'avg_prcp')
    
    print_stars()
    
    print("                  Before transformation")
    print("\n                  Test for Normal Distribution: \n")

    print("       Popular Cities Data: ")

    check_normality(popular_cities_df, 'avg_tmax')
    check_normality(popular_cities_df, 'avg_tmin')
    check_normality(popular_cities_df, 'avg_prcp')

    print("\n       Unpopular Cities Data: ")

    check_normality(unpopular_cities_df, 'avg_tmax')
    check_normality(unpopular_cities_df, 'avg_tmin')
    check_normality(unpopular_cities_df, 'avg_prcp')
    
    print("\n      For Combined Cities DataFrame (Popular & Unpopular):\n")

    check_normality(combined_cities_df, 'avg_tmax')
    check_normality(combined_cities_df, 'avg_tmin')
    check_normality(combined_cities_df, 'avg_prcp')

    print("\n      For Random Cities DataFrame:\n")

    check_normality(random_cities_df, 'avg_tmax')
    check_normality(random_cities_df, 'avg_tmin')
    check_normality(random_cities_df, 'avg_prcp')
    
    print_stars()

    print("\n                  Test for Equal Variances: \n")
    check_equal_variances(combined_cities_df, 'avg_tmax')
    check_equal_variances(combined_cities_df, 'avg_tmin')
    check_equal_variances(combined_cities_df, 'avg_tmin')


    
    print_stars()

    print("\n      T-Tests:\n")
    print("Popular cities and unpopular cities - Maximum Temperature: ")
    do_ttest(popular_cities_df, unpopular_cities_df, 'avg_tmax')
    print("\nPopular cities and unpopular cities - Minimum Temperature: ")
    do_ttest(popular_cities_df, unpopular_cities_df, 'avg_tmin')
    print("\nPopular cities and unpopular cities - Precipitation: ")
    do_ttest(popular_cities_df, unpopular_cities_df, 'avg_prcp')

    # plot graphs
    plot_combined_histograms(combined_cities_df)
    plot_random_histograms(random_cities_df)

if __name__ == "__main__":
    #  python3 infer_stats.py popular_cities_out/ unpopular_cities_out/ random_cities_out/
    in_directory_1 = sys.argv[1]
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]

    main(in_directory_1, in_directory_2, in_directory_3)
