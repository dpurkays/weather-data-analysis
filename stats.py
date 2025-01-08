import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def read_json_directory(directory):
    dataframes = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            df = pd.read_json(file_path, lines=True)
            dataframes.append(df)
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

def check_figures_dir():
    if not os.path.exists("figures"):
        os.makedirs("figures")

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

    plt.savefig(f"figures/stats_histograms.png", bbox_inches='tight')


def plot_boxplots(popular_cities_df, unpopular_cities_df, random_cities_df):
    tmax_popular = popular_cities_df['avg_tmax']
    tmax_unpopular = unpopular_cities_df['avg_tmax']
    tmax_random = random_cities_df['avg_tmax']

    tmin_popular = popular_cities_df['avg_tmin']
    tmin_unpopular = unpopular_cities_df['avg_tmin']
    tmin_random = random_cities_df['avg_tmin']

    prcp_popular = popular_cities_df['avg_prcp']
    prcp_unpopular = unpopular_cities_df['avg_prcp']
    prcp_random = random_cities_df['avg_prcp']

    # Boxplots for tmax and tmin
    plt.figure(figsize=(10, 6))
    plt.subplot(2, 1, 1)
    plt.boxplot([tmax_popular, tmax_unpopular, tmax_random],
                labels=['Popular', 'Unpopular', 'Random'])
    plt.title('Boxplot of Average Maximum Temperature')
    plt.ylabel('Average Max Temperature (°C)')

    plt.subplot(2, 1, 2)
    plt.boxplot([tmin_popular, tmin_unpopular, tmin_random],
                labels=['Popular', 'Unpopular', 'Random'])
    plt.title('Boxplot of Average Minimum Temperature')
    plt.ylabel('Average Min Temperature (°C)')

    plt.tight_layout()
    
    check_figures_dir()
    
    plt.savefig(f"figures/boxplot_temp.png")

    # Boxplot for prcp
    plt.figure(figsize=(8, 6))
    plt.boxplot([prcp_popular, prcp_unpopular, prcp_random],
                labels=['Popular', 'Unpopular', 'Random'])
    plt.title('Boxplot of Average Precipitation')
    plt.ylabel('Average Precipitation in mm')
    
    plt.savefig(f"figures/boxplot_prcp.png")
    plt.close()



def main(in_directory_1, in_directory_2, in_directory_3):
    popular_cities_df = read_json_directory(in_directory_1)
    popular_cities_df['is_popular'] = True

    unpopular_cities_df = read_json_directory(in_directory_2)
    unpopular_cities_df['is_popular'] = False

    random_cities_df = read_json_directory(in_directory_3)

    print("                  *** Stats ***")
    print("\n            Popular Cities: ")
    print("\nMeans: ")
    print("tmax", popular_cities_df['avg_tmax'].mean().round(2))
    print("tmin", popular_cities_df['avg_tmin'].mean().round(2))
    print("prcp", popular_cities_df['avg_prcp'].mean().round(2))

    print("\nStandard Deviations: ")
    print("tmax", popular_cities_df['avg_tmax'].std().round(2))
    print("tmin", popular_cities_df['avg_tmin'].std().round(2))
    print("prcp", popular_cities_df['avg_prcp'].std().round(2))

    print("\n            Unpopular Cities: ")
    print("\nMeans: ")
    print("tmax", unpopular_cities_df['avg_tmax'].mean().round(2))
    print("tmin", unpopular_cities_df['avg_tmin'].mean().round(2))
    print("prcp", unpopular_cities_df['avg_prcp'].mean().round(2))

    print("\nStandard Deviations: ")
    print("tmax", unpopular_cities_df['avg_tmax'].std().round(2))
    print("tmin", unpopular_cities_df['avg_tmin'].std().round(2))
    print("prcp", unpopular_cities_df['avg_prcp'].std().round(2))

    print("\n            Random Cities: ")
    print("\nMeans: ")
    print("tmax", random_cities_df['avg_tmax'].mean().round(2))
    print("tmin", random_cities_df['avg_tmin'].mean().round(2))
    print("prcp", random_cities_df['avg_prcp'].mean().round(2))

    print("\nStandard Deviations: ")
    print("tmax", random_cities_df['avg_tmax'].std().round(2))
    print("tmin", random_cities_df['avg_tmin'].std().round(2))
    print("prcp", random_cities_df['avg_prcp'].std().round(2))

    # plot graphs
    plot_hist(popular_cities_df, unpopular_cities_df)
    plot_boxplots(popular_cities_df, unpopular_cities_df, random_cities_df)



if __name__ == "__main__":
    #  python3 stats.py popular_cities_out/ unpopular_cities_out random_cities_out
    in_directory_1 = sys.argv[1]
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]

    main(in_directory_1, in_directory_2, in_directory_3)
