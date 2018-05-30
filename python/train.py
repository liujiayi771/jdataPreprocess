import pandas as pd
import numpy as np
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
import pickle
import datetime
import time

def train_with_xgboost(train_x ,train_y):
    dump_path = 'xgboost.pkl'
    param = {'max_depth': 3, 'learning_rate': 0.1,
                    'n_estimators': 100, 'silent': True,
                    'objective': "binary:logistic",
                    'gamma': 0, 'min_child_weight': 1, 'max_delta_step': 0, 'subsample': 1,
                    'colsample_bytree': 1, 'colsample_bylevel': 1,
                    'reg_alpha': 0, 'reg_lambda': 1, 'scale_pos_weight': 1,
                    'base_score': 0.5, 'seed': 123}
    train_model = XGBClassifier(param)
    train_model.fit(X=train_x, y=train_y)
    pickle.dump(train_model, open(dump_path, 'w'))

    return train_model

def train_with_lr(train_x, train_y):
    train_model = LogisticRegression()
    train_model.fit(X=train_x, y=train_y)
    return train_model

from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso

def train_with_ridge(train_x, train_y):
    clf = Ridge(alpha=1.0)
    clf.fit(train_x, train_y)
 #   pred_date = clf.predict(val_x)

def train_with_lasso(train_x, train_y):
    clf = Lasso(alpha=0.1)
    clf.fit(train_x, train_y)
#    pred_date = clf.predict(val_x)



def train(f1_path, l_path, f2_path):

    start_date = '2017-02-01'
    train_x = pd.read_csv(f1_path)
    train_y = pd.read_csv(l_path)
    val_x = pd.read_csv(f2_path)

    clf_model = train_with_xgboost(train_x ,train_y['user'])
    val_user = clf_model.predict_proba(val_x)
    train_y['prob'] = pd.DataFrame(val_user)
    train = pd.concat([train_x, train_y])
    train.sort_values(key = 'prob')
    train = train.head(50000)
    x = train.drop(['date', 'user'])
    y = train[['user']]
    reg_model = train_with_ridge(x,y)
    val_date = reg_model.predict(reg_model)
    result = pd.concat(train['user_id'], pd.DataFrame(val_date,columns='date'))
    result.sort_values(key = 'date')
    result['date'].map(lambda x: time.strftime("%Y-%m-%d",
                                               datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(days=x)))
    result.to_csv('submission.csv',index=False, index_label=False)




