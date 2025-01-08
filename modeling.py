import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml import Pipeline

from sklearn.model_selection import train_test_split
from sklearn.ensemble import VotingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier


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


def plot_bar_predictions(data):

    plt.figure(figsize=(8, 6))

    actual_counts = data['is_popular'].value_counts()
    predicted_counts = data['predicted_popularity'].value_counts()

    plt.bar(actual_counts.index, actual_counts, color='gray',
            alpha=0.7, width=0.4, align='edge', label='Actual')
    plt.bar(predicted_counts.index + 0.4, predicted_counts, color='green',
            alpha=0.7, width=0.4, align='edge', label='Predicted')

    plt.xlabel('Popularity')
    plt.xticks([0.2, 1.2], ['Unpopular', 'Popular'])
    plt.ylabel('Count')
    plt.title('Actual vs Predicted Popularity')
    plt.legend()

    check_figures_dir()
    plt.savefig(f"figures/bar_prediction_comparison.png")


def test_data_prediction(model, df):
    test_data = df.drop(['name', 'station'], axis=1)
    predictions = model.predict(test_data.values)

    test_data['is_popular'] = predictions
    test_data['name'] = df['name']
    test_data['station'] = df['station']

    # reorder the columns
    test_data = test_data[['name', 'station', 'avg_tmax',
                           'avg_tmin', 'avg_prcp', 'is_popular']]

    output_file = "test_data_predictions.json"
    test_data.to_json(output_file, orient='records', lines=True)
    print("Output file saved to:", output_file)


def print_stars():
    char = '*'
    repetitions = 60
    print(char * repetitions)


def main(in_directory_1, in_directory_2, in_directory_3):  # , out_directory):
    popular_cities_data = read_json_directory(in_directory_1)
    popular_cities_data['is_popular'] = True
    # print(popular_cities_df)

    unpopular_cities_data = read_json_directory(in_directory_2)
    unpopular_cities_data['is_popular'] = False

    random_cities_df = read_json_directory(in_directory_3)

    # join popular and unpopular cities
    frames = [popular_cities_data, unpopular_cities_data]
    cities_data = pd.concat(frames)
    # print(cities_data)

    # make train and valid data
    X = cities_data[['avg_tmax', 'avg_tmin', 'avg_prcp']].values
    y = cities_data['is_popular'].values
    # print(X)
    # print(y)

    X_train, X_valid, y_train, y_valid = train_test_split(
        X, y, test_size=0.2, random_state=42)

    model = VotingClassifier([
        ('nb', GaussianNB()),
        ('knn', KNeighborsClassifier()),
        ('svm', SVC(kernel='linear', C=0.1)),
        ('tree1', DecisionTreeClassifier(max_depth=4)),
        ('tree2', DecisionTreeClassifier(min_samples_leaf=10))
    ])
    model.fit(X_train, y_train)
    predictions = model.predict(X_valid)

    print("\n")
    print_stars()
    print("Training Score: ", model.score(X_train, y_train))
    print("Validation Score: ", model.score(X_valid, y_valid))
    print_stars()
    print("\n")

    X_valid_df = pd.DataFrame(
        X_valid, columns=['avg_tmax', 'avg_tmin', 'avg_prcp'])

    X_valid_df['is_popular'] = y_valid
    X_valid_df['predicted_popularity'] = predictions

    plot_bar_predictions(X_valid_df)

    test_data_prediction(model, random_cities_df)
    # visualize_results(predictions)


if __name__ == "__main__":
    # python3 modeling2.py popular_cities_out/ unpopular_cities_out/ random_cities_out/
    in_directory_1 = sys.argv[1]
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]

    main(in_directory_1, in_directory_2, in_directory_3)
