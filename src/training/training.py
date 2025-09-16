from linear_regression import linear_regression_training
from random_forest import random_forest_training
from xgboost_train import xgboost_training


def training_models():
    lr = linear_regression_training()
    rf = random_forest_training()
    xgb = xgboost_training()


training_models()
