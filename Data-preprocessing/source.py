import pandas as pd
import numpy as np
import math
import sys

df = pd.read_csv(sys.argv[1], sep =",")
data = df.to_numpy().tolist()
header = df.columns.to_numpy().tolist()

def is_nan(a):
    return isinstance(a, float) and a!=a

def list_missing_data_columns(data, header):
    ## Use data type set() to remove duplicate header columns
    ## Interating all the data, check and add to set()
    ans = set()
    for row in  range(len(data)):
        for i in range(len(data[row])):
            if is_nan(data[row][i]):
                ans.add(header[i])
    return ans

def count_row_missing_value(data):
    ##Iterating all the data, if the row has the missing value, add 1 to ans and break, continue to the end of data
    ans = 0
    for row in data:
        for i in range(len(row)):
            if is_nan(row[i]):
                ans = ans + 1
                break
    return ans

def fill_missing_value_columns(dff, attribute, type_):
    ## Caculating the mean, median or mode folowing the request
    ## Iterating all the data, if item in data is missing-value, then assign it to the caculation results
    df = dff.copy()
    data = df.to_dict('split')['data']
    data = np.array(data).T
    index = None
    for i in range(len(header)):
        if header[i] == attribute:
            index = i
            break
    
    
    data_attribute = df[attribute].to_numpy().tolist()
    ans = None
    ## If the data type of attribute is numeric, we can choose all method {mean, meidan, mode}, but if it is nominal, we just choose mode for it
    def get_mean():
        count = 0
        sum_ = 0
        for item in data_attribute:
            if not is_nan(item):
                sum_ += item
                count += 1
        return sum_/count
    
    def get_median():
        data_temp = []
        for item in data_attribute:
            if not is_nan(item):
                data_temp.append(item)
        data_temp.sort()
        if len(data_temp)%2 == 1:
            return data_temp[len(data_temp)//2]
        else:
            return (data_temp[len(data_temp)//2] + data_temp[len(data_temp)//2 + 1])/2

    def get_mode():
        data_temp = {}
        for item in data_attribute:
            if not is_nan(item):
                data_temp[item] = data_temp.get(item, 0) + 1
        max_ = -100
        ans = None
        for item in data_temp:
            if data_temp[item] > max_:
                max_ = data_temp[item]
                ans = item
        return ans

    if type_ == "mean":
        try:
            ans = get_mean()
        except:
            ans = get_mode()
    elif type_ == "median":
        try:
            ans = get_median()
        except:
            ans = get_mode()
    elif type_ == "mode":
        ans = get_mode()
    else:
        print('Wrong type')
        return

    for i in range(len(data[index])):
        if data[index][i] == 'nan':
            data[index][i] = ans
    #print(data[index])
    data = data.T
    df = pd.DataFrame(data)
    df.columns = header
    return df

def export_csv(df, path_name):
    df = df.replace('nan', '')
    df.to_csv(path_name, index = False)

def delete_row(df, rate):
    ## check each row of the data, counting the missing value in each row, if the count is greater than rate, then remove this row out of the data 
    row = []
    #print(type(rate))
    data = df.to_numpy().tolist()
    header_ = df.columns.to_numpy().tolist()
    i = 0
    while (i <len(data)):
        count = 0
        for j in range(len(data[i])):
            if is_nan(data[i][j]):
                count += 1
        if count/len(data[i]) > rate:
            data.pop(i)
            i -=1
        i+=1
        
    dk = pd.DataFrame(data)
    dk.columns = header_
    return dk

def delete_col(df, rate):
    ## check each column of the data, counting the missing value in each column, if the count is greater than rate, then remove this row out of the data
    data = df.to_numpy().T.tolist()
    header_ = df.columns.to_numpy().tolist()
    i = 0
    while (i <len(data)):
        count = 0
        for j in range(len(data[i])):
            if is_nan(data[i][j]):
                count += 1
        if count/len(data[i]) > rate:
            data.pop(i)
            header_.pop(i)
            i -=1
        i+=1
    data = np.array(data).T    
    dk = pd.DataFrame(data)
    dk.columns = header_
    return dk

def delete_duplicate(df):
    ## Add all rows to set() to remove all duplicate rows, but before add to set(), we convert the rows from list to tuple for easy to handle
    data = df.to_numpy().tolist()
    header_ = df.columns.to_numpy().tolist()

    set_data = set(tuple(x) for x in data)
    list_data = list(list(x) for x in set_data)

    dk = pd.DataFrame(list_data)
    dk.columns = header_
    return dk

def standard_min_max(df, attr):
    # Caculation min, max in the columns'attribution, then continue calculating the standard value of each element in the columns
    # Then assign to the data and convert to pandas.DataFrame() to export file csv
    data_full = df.to_numpy().T.tolist()
    header_ = df.columns.to_numpy().tolist()
    index = None
    for i in range (len(header_)):
        if (header_[i] == attr):
            index = i
            break
    data = data_full[index].copy()
    #check numeric and pop nan value
    j = 0
    isNumber = True
    while (j<len(data)):
        if not is_nan(data[j]):
            if (type(data[j])!= int and type(data[j])!= float):
                isNumber = False
                break
        else:
            data.pop(j)
            j-=1
        j+=1
    
    #if data null, so the columns is null
    if len(data) == 0:
        print('Columns null')
        return None
    if not isNumber:
        print('This attribute is not numeric')
        return None
    else:
        a = min(data)
        b = max(data)
        for i in range(len(data_full[index])):
            if not is_nan(data_full[index][i]):
                data_full[index][i] = (data_full[index][i] - a)/(b-a)
    data_full = np.array(data_full).T
    dk = pd.DataFrame(data_full)
    dk.columns = header_
    #print(dk)
    return dk

def standard_Zscore(df, attr):
    # Caculation the mean and standard deviation in the columns'attribution, then continue calculating the standard value of each element in the columns
    # Then assign to the data and convert to pandas.DataFrame() to export file csv
    data_full = df.to_numpy().T.tolist()
    header_ = df.columns.to_numpy().tolist()
    index = None
    for i in range (len(header_)):
        if (header_[i] == attr):
            index = i
            break
    data = data_full[index].copy()
    #check numeric and pop nan value
    j = 0
    isNumber = True
    while (j<len(data)):
        if not is_nan(data[j]):
            if (type(data[j])!= int and type(data[j])!= float):
                isNumber = False
                break
        else:
            data.pop(j)
            j-=1
        j+=1
    
    #if data null, so the columns is null
    if len(data) == 0:
        print('Columns null')
        return None
    if not isNumber:
        print('This attribute is not numeric')
        return None
    else:
        mean = sum(data)/len(data)
        var = 0
        for i in range(len(data)):
            var += (data[i] - mean)**2
        var /= len(data)
        var = var**(0.5)
        #variance equal to zero, so all value in data equal to mean
        if var == 0:
            for i in range(len(data_full[index])):
                if not is_nan(data_full[index][i]):
                    data_full[index][i] = mean
        else:
            for i in range(len(data_full[index])):
                if not is_nan(data_full[index][i]):
                    data_full[index][i] = (data_full[index][i] - mean)/var
    data_full = np.array(data_full).T
    dk = pd.DataFrame(data_full)
    dk.columns = header_
    return dk

def calculation_attr(df, req):
    data_full = df.to_numpy().T
    header_ = df.columns.to_numpy().tolist()
    ##Get attribute to calculate in the request
    attr = []
    for i in range(len(header_)):
        if header_[i] in req:
            attr.append(i)
    if (len(attr) < 2):
        print("The number of attributes must greater to 2 for calculating!")
        return
    ##Check attribute's data, if the data type is nominal or missing-value, return false, else return true 
    def check(index_attr):
        data_attr = data_full[index_attr].copy()
        for i in range(len(data_attr)):
            if is_nan(data_attr[i]):
                return False
            if type(data_attr[i])!= int and type(data_attr[i])!=float:
                return False
        return True
    ##Create a dictionary for eval() function
    data_attr = {}
    for i in range(len(attr)):
        if check(attr[i]):
            data_attr[header_[attr[i]]] = data_full[attr[i]].copy()
        else:
            print('The type of data is nominal or the columns have a missing-value')
            return
    data_full = data_full.tolist()
    new_col = eval(req, data_attr)
    
    new_col = new_col.tolist()
    data_full.append(new_col)
    data_full = np.array(data_full).T
    header_.append(req)
    dk = pd.DataFrame(data_full)
    dk.columns = header_
    return dk
        
    
            
#test = standard_Zscore(df, 'LotFrontage')
#export_csv(test, 'asdf.csv')
if __name__ == '__main__':


    ## Agrv for list-missing : python3 <file.py> <input_file.csv> list-missing
    if (sys.argv[2] == 'list-missing'):
        print("List of missing-data columns:")
        print(list_missing_data_columns(data, header))
    ## Argv for count-missing : python3 <file.py> <input_file.csv> count-missing
    elif (sys.argv[2] == 'count-missing'):
        print('The number of missing-data rows: \t', end='')
        print(count_row_missing_value(data))
    ## Argv for filling-missing : python3 <file.py> <input_file.csv> filling-missing <attribute> <method> <file_out>
    ## method = {mean, mode, median}
    elif (sys.argv[2] == 'filling-missing'):
        print('Handling....')
        export_csv(fill_missing_value_columns(df, sys.argv[3], sys.argv[4]), sys.argv[5])
        print('Done!')
    ## Argv for deleting-rows : python3 <file.py> <input_file.csv> deleting-rows <rate> <file_out>
    elif (sys.argv[2] == 'deleting-rows'):
        print('Handling...')
        rate = float(sys.argv[3])
        export_csv(delete_row(df, rate), sys.argv[4])
        print('Done!')
    ## Argv for deleting-rows : python3 <file.py> <input_file.csv> deleting-cols <rate> <file_out>
    elif (sys.argv[2] == 'deleting-cols'):
        print('Handling...')
        rate = float(sys.argv[3])
        export_csv(delete_col(df, rate), sys.argv[4])
        print('Done!')
    ## Argv for deleting-duplicate : python3 <file.py> <input_file.csv> deleting-duplicate <file_out>
    elif (sys.argv[2] == 'deleting-duplicate'):
        print('Handling')
        export_csv(delete_duplicate(df), sys.argv[3])
        print('Done!')
    ## Argv for deleting-duplicate : python3 <file.py> <input_file.csv> standardizing <attribute> <method> <file_out>
    ## <method> = {z_score, min_max}
    elif (sys.argv[2] == 'standardizing'):
        if (sys.argv[4] == 'z-score'):
            print('Handling')
            result = standard_Zscore(df, sys.argv[3])
            export_csv(result, sys.argv[5])
            print('Done')
        elif (sys.argv[4] == 'min-max'):
            print('Handling')
            result = standard_min_max(df, sys.argv[3])
            export_csv(result, sys.argv[5])
            print('Done')
    ## Argv for deleting-duplicate : python3 <file.py> <input_file.csv> calculating-attr <request> <file_out>
    elif (sys.argv[2] == 'calculating-attr'):
        print('Handling')
        export_csv(calculation_attr(df, sys.argv[3]), sys.argv[4])
        print('Done!')
   


























    




