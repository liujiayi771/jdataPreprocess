import numpy as np
import pandas as pd
from datetime import  datetime
def eval_user(pred, label):
    N = pred.size()
    w = np.array([1/(1+np.log(i)) for i in range(1,N+1)])
    ind = np.array([pred == label])
    return np.dot(w,ind)/np.sum(w)

def eval_date(pred, label):
    result = pd.merge(pred, label, how ='left', on = 'user_id')
    result = result[[result['pred_date']!= np.NaN and result['true_date']!= np.NaN]]
    result['score'] =result.apply( (datetime.strptime(result['pred_date'], '%Y-%m-%d') -
                                   datetime.strptime(result['true_date'], '%Y-%m-%d')).days)
    result['score'] = result['score'].map(lambda x: 10/(10 + x*x))
    return result['score'].sum()/result['true_label'].count()

def eval(pred, label):
    return 0.4 * eval_user(pred['user_id'], label['user_id']) + 0.6 *(eval_date(pred, label))
