import pandas as pd
import numpy as np
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
import pandas as pd
import pickle
import datetime
import time
import os

def train_with_xgboost(train_x ,train_y):
    dump_path = 'xgboost.pkl'
    param = {'max_depth': 3, 'learning_rate': 0.1,
                    'n_estimators': 100, 'silent': True,
                    'objective': "binary:logistic",
                    'gamma': 0, 'min_child_weight': 1, 'max_delta_step': 0, 'subsample': 1,
                    'colsample_bytree': 1, 'colsample_bylevel': 1,
                    'reg_alpha': 0, 'reg_lambda': 1, 'scale_pos_weight': 1,
                    'base_score': 0.5, 'seed': 123}
    train_model = XGBClassifier(**param)
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
    return clf
 #   pred_date = clf.predict(val_x)

def train_with_lasso(train_x, train_y):
    clf = Lasso(alpha=0.1)
    clf.fit(train_x, train_y)
#    pred_date = clf.predict(val_x)



def train(f1_path, l_path, f2_path):

    feature_start= '2017-04-01'
    label_start = '2017-05-01'
    train_x = pd.read_csv(f1_path)
    train_y = pd.read_csv(l_path)
    val_x = pd.read_csv(f2_path)


    train = pd.merge(train_x, train_y, how='left', on='user_id')

    #train.drop((pd.isna(train['bought'])), inplace=True)

   # train['bought'].map(lambda x: ( 0 if pd.isna(x) else x))
    train = train[np.isnan(train['bought']) == False]
    clf_model = train_with_xgboost(train[['score_level_1','score_level_2',
                                           'score_level_3','o_sku_sum','action_1','action_2','o_area','max_count']],train['bought'])
    #val_x = val_x[['score_level_1','score_level_2',
    #                                       'score_level_3','o_sku_sum','action_1','action_2','o_area','max_count']]
    val_prob = clf_model.predict_proba(val_x[['score_level_1','score_level_2',
                                          'score_level_3','o_sku_sum','action_1','action_2','o_area','max_count']])[:,1]
    val_x['prob'] = pd.DataFrame(val_prob)
    val_x['bought'] = val_x['prob'].map(lambda x: 1 if x >= 0.5 else 0)
    val_x = val_x.sort_values(by = ['prob'], ascending = False)
    val_user = val_x['user_id']

    val_x = val_x.drop(['user_id', 'prob'], axis=1)
    #val_x = val_x.head(50000)
    # train = train.fillna(0)
    x = train.drop(['user_id','earliest_date'], axis =1)
    y = train['earliest_date'].map(
        lambda x: (datetime.datetime.strptime(x, '%Y-%m-%d') - datetime.datetime.strptime(feature_start, '%Y-%m-%d')).days if not pd.isna(x) else np.nan)
    x = x.fillna(0)
    y = y.fillna(30)
    val_x = val_x.fillna(0)
    reg_model = train_with_ridge(x,y)
    val_date = reg_model.predict(val_x)
    result = pd.concat([val_user, pd.DataFrame(val_date,columns=['date'])], axis = 1)
    result['date'] =  result['date'].fillna(30)

    result['date'] = result['date'].map(lambda x: abs(int(x)))
    result = result.sort_values(by=['date'])
    result['date'] = result['date'].map(
        lambda x: (datetime.datetime.strptime(label_start, '%Y-%m-%d') + datetime.timedelta(days=x)).strftime("%Y-%m-%d") )
    result = result.head(50000)


    result.to_csv('./submission.csv',index=False, index_label=False, header = ['user_id','pred_date'])


outputpath = "/Users/chenghanni/Documents/data"

f1_start = "2016-07-01"
f1_end = "2017-04-01"
f2_start = "2016-08-01"
f2_end = "2017-05-01"
l1_start = "2017-04-01"
l1_end = "2017-05-01"

f1_path = os.path.join(outputpath ,"feat_"+ f1_start + "_" + f1_end, "f.csv")
f2_path = os.path.join(outputpath ,"feat_"+ f2_start + "_" + f2_end, "f.csv")
l_path = os.path.join(outputpath ,"label_"+ l1_start + "_" + l1_end, "l.csv")
train(f1_path, l_path, f2_path)
