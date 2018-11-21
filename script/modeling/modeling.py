#!/usr/bin/env python3

import csv
import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt 
import argparse
from sklearn.model_selection import train_test_split  
from sklearn.linear_model import LinearRegression  
from sklearn.kernel_ridge import KernelRidge
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics  
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network.multilayer_perceptron import MLPRegressor

'''
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
'''


def WriteResult(path, label, data):
    with open(path, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([label] + list(data))


def Main(method, mode, input_path, output_path):
    dataset = pd.read_csv(input_path)  
    print(dataset.shape)
    print(dataset.iloc[:, -8:-4].describe(include='all'))
    
    x = dataset.iloc[:, :5].values  
    y = dataset.iloc[:, -8:-4].values
    
    if mode == 'sum':
       y = np.sum(y, axis=1) 
    
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)  

    scaler = StandardScaler()
    scaler.fit(x_train)
    x_train = scaler.transform(x_train)
    x_test = scaler.transform(x_test)

    if method == 'lr':
        regressor = LinearRegression()  
    if method == 'kr':
        regressor = KernelRidge(kernel='rbf', gamma=0.1)  
    if method == 'rf':
        regressor = RandomForestRegressor(n_estimators=100, n_jobs=4)  
    if method == 'nn':
        regressor = MLPRegressor(hidden_layer_sizes=(100, 100), max_iter=10000)

    regressor.fit(x_train, y_train)  
    
    # print("coefficients:\n", regressor.coef_)
    
    evaluate_data = [(x_train, y_train), (x_test, y_test)]
    labels = ["Train", "Test"]
    for i, data in enumerate(evaluate_data):
        label = labels[i]

        y_pred = regressor.predict(data[0])  
        
        print("\n{} Error:".format(label))
        mae = metrics.mean_absolute_error(data[1], y_pred, multioutput='raw_values')
        WriteResult(output_path, "{} {} mae".format(method, label.lower()), mae)
        print('Mean Absolute Error:', mae)  
        rmse = np.sqrt(metrics.mean_squared_error(data[1], y_pred, multioutput='raw_values'))
        print('Root Mean Squared Error:', rmse)  
        WriteResult(output_path, "{} {} rmse".format(method, label.lower()), rmse)

    print()
    
    test_rmse = np.sqrt(metrics.mean_squared_error(y_test, y_pred, multioutput='raw_values'))
    x_test = scaler.inverse_transform(x_test)
    for i, x_prime in enumerate(x_test):
        y_prime = y_test[i]
        y_hat = y_pred[i]
        if np.isscalar(y_hat):
            std = test_rmse[0]
            if abs(y_hat - y_prime) > std:
                print('x:{} y:{} y_hat:{}'.format(x_prime, y_prime, y_hat))
        else:
            for j, std in enumerate(test_rmse):
                if abs(y_hat[j] - y_prime[j]) > std:
                    print('dim:{} x:{} y:{} y_hat:{}'.format(j, x_prime, y_prime[j], y_hat[j]))


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Contention Modeling')
    aparser.add_argument('--method', default='lr', help='ML method')
    aparser.add_argument('--mode', choices=['sum', 'raw'], default='raw', help='Whether to sum all targets')
    aparser.add_argument('--input_path', help='Input file path')
    aparser.add_argument('--output_path', help='Output file path')
    args = vars(aparser.parse_args())

    Main(args['method'], args['mode'], args['input_path'], args['output_path'])
