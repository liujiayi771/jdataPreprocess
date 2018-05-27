import pandas as pd
import os
import numpy as np
import pickle
from datetime import datetime

rootpath = "data/"

info_path = os.path.join(rootpath, "jdata_user_basic_info.csv")
product_path = os.path.join(rootpath, "jdata_sku_basic_info.csv")
action_path = os.path.join(rootpath,"jdata_user_action.csv")

comment_path = os.path.join(rootpath, "jdata_user_comment_score.csv")
order_path = os.path.join(rootpath, "jdata_user_order.csv")

def convert_age(age):
    if age == 1:
        return 0
    elif age <16:
        return 1
    elif age < 26:
        return 2
    elif age < 36:
        return 3
    elif age < 46:
        return 4
    elif age< 56:
        return 5
    elif age >= 56:
        return 6
    else:
        return -1

def most_common(x):
    print x
    return np.argmax(np.bincount(x['o_area'].as_matrix()))

def get_basic_user_feat():
    dump_path = 'basic_user.pkl'
    if os.path.exists(dump_path):
        user = pickle.load(open(dump_path))
    else:
        user = pd.read_csv(info_path, encoding='gbk')
        user['age'] = user['age'].map(convert_age)
        age_df = pd.get_dummies(user["age"], prefix="age")
        sex_df = pd.get_dummies(user["sex"], prefix="sex")
        user_lv_df = pd.get_dummies(user["user_lv_cd"], prefix="user_lv_cd")
        user = pd.concat([user['user_id'], age_df, sex_df, user_lv_df], axis=1)
        pickle.dump(user, open(dump_path, 'w'))
    return user


def get_action_feat(start_date, end_date):
    """
        :param start_date:
        :param end_date:
        :return: actions: pd.Dataframe
        """
    dump_path = 'action_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        action = pickle.load(open(dump_path))
    else:
        action = pd.read_csv(action_path)
        print (action.head(20))
        action = action[(action.a_date >= start_date) & (action.a_date < end_date)]
        action = action[['user_id', 'a_type', 'a_num']]
        df = pd.get_dummies(action['a_type'], prefix='%s-%s-action' % (start_date, end_date))
        action = pd.concat([action, df], axis=1)  # type: pd.DataFrame
        action['%s-%s-action_1' % (start_date, end_date)] = map(lambda x, y: x * y,
                                                                action['%s-%s-action_1' % (start_date, end_date)],
                                                                action['a_num'])
        action['%s-%s-action_2' % (start_date, end_date)] = map(lambda x, y: x * y,
                                                                action['%s-%s-action_2' % (start_date, end_date)],
                                                                action['a_num'])
        del action['a_type']
        del action['a_num']
        action = action.groupby(['user_id'], as_index=False).sum()
        print(action.head(20))
        pickle.dump(action, open(dump_path, 'w'))
    return action


def get_comments_product_feat(start_date, end_date):
    dump_path = 'omments_accumulate_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        comments = pickle.load(open(dump_path))
    else :
        comments = pd.read_csv(comment_path)
        comments = comments[(comments.comment_create_tm >= start_date) & (comments.comment_create_tm < end_date)]
        df = pd.get_dummies(comments['score_level'], prefix = "score_level")
        comments = pd.concat([comments, df], axis=1)

        del comments['score_level']
        del comments['o_id']
        pickle.dump(comments, open(dump_path, 'w'))
    return comments


def get_area_feat():
    dump_path = 'area.pkl'
    if os.path.exists(dump_path):
        area = pickle.load(open(dump_path))
    else:
        area = pd.read_csv(order_path)
        area = area[['user_id','o_area']]
        area = area.groupby(area['user_id'], as_index=False).agg(lambda x: np.argmax(np.bincount(np.array(x['o_area']))))
        #pickle.dump(area, open(dump_path, 'w'))
        print(area)
    return area

def get_order_feat(start_date, end_date):
    dump_path = 'order.pkl'
    if os.path.exists(dump_path):
        orders = pickle.load(open(dump_path))
    else:
        orders = pd.read_csv(order_path)
        orders = orders[(orders.o_date >= start_date) & (orders.o_date < end_date)]
        orders = orders[['user_id', 'o_sku_num', 'o_date']]
        orders = orders.groupby(['user_id', 'o_sku_num'], as_index=False).min()
        orders = orders.groupby(['user_id' ,'o_date'], as_index=False).sum()
        print(orders.head(20))
        pickle.dump(orders, open(dump_path, 'w'))
    return orders

def merge_feat(train_start_date, train_end_date):
    dump_path = 'merge_%s_%s.pkl' % (train_start_date, train_end_date)
    if os.path.exists(dump_path):
        feats = pickle.load(open(dump_path))
    else:
        start_days = "2016-02-01"
        users = get_basic_user_feat()
      #  product = get_basic_product_feat()
        actions = get_action_feat()
        areas = get_area_feat()
        comments = get_comments_product_feat(train_start_date, train_end_date)
        orders = get_order_feat(train_start_date, train_end_date)

        feats = pd.merge(users, actions, how='left', on='user_id')
        feats = pd.merge(feats, areas, how='left', on='user_id')
        feats = pd.merge(feats, comments, how='left', on='user_id')
        feats = pd.merge(feats, orders, how='left', on='user_id')
        feats = feats.fillna(0)
    return feats

def get_labels(start_date, end_date):
    users = get_basic_user_feat()
    orders = get_order_feat(start_date, end_date)
    orders = orders[['user_id','o_date']]
    orders['o_date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d'))
    orders['label'] = 1
    users = users[['user_id']]
    labels = pd.merge(users, orders, how='left', on = 'user_id')
    print(labels.head(10))



start_date = '2017-02-01'
end_date = '2017-03-01'
#dump_path = 'action_%s_%s.pkl' % (start_date, end_date)
#get_action_feat(start_date, end_date)
get_labels(start_date, end_date)

