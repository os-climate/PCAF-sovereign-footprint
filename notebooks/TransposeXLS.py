import pandas as pd
import numpy as np
import sys, getopt
import configparser
from sys import argv
from datetime import datetime
from sqlalchemy import types, create_engine
import csv
import argparse
import re
import os
import openpyxl



def eval_formula(eval_components, count=0, sheet='Sheet1'):
    print("eval_components")
    print(eval_components)
    columns = list(df_collection[count])
    print(columns)
    eval_str = ''
    i = 0
    for eval_component in eval_components:
        print(eval_component)
        i = i + 1
        if i == 1:
            if eval_component == "$sheet_name":
                eval_str = sheet
            elif eval_component in columns:
                eval_str = 'df_collection[' + str(count) + ']' + '[\'' + eval_component + '\']'
            else:
                eval_str = eval_component
        else:
            if eval_component == "$sheet_name":
                eval_str = eval_str + '+' + sheet
            elif eval_component in columns:
                eval_str = eval_str + '+' + 'df_collection[' + str(count) + ']' + '[\'' + eval_component + '\']'
            else:
                eval_str = eval_str + '+' + eval_component
    return eval_str


# Replace the specific part of the string based on pattern
def execute_replace_patterns_on_row(row, replace_patterns):
    for replace_pattern in replace_patterns:
        pattern_1 = replace_pattern.split(':')[0]
        pattern_2 = replace_pattern.split(':')[1]
        print(pattern_1)
        print(pattern_2)
        for index, col_val in enumerate(row):
            col_val = col_val.replace('"', "")
            col_val = col_val.replace('\'', "###")
            eval_txt = 're.sub(\'' + pattern_1 + '\',\'' + pattern_2 + '\',\'' + col_val + '\')'
            col_val = eval(eval_txt)
            col_val = col_val.replace('###', '\'')
            row[index] = col_val
    return row


def dequote(s):
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s

def splitstring(l, splitchar, ignorechar):
    result = []
    string = ""
    ignore = False
    for c in l:
        if c == ignorechar:
            ignore = True if ignore == False else False
        elif c == splitchar and not ignore:
            result.append(string)
            string = ""
        else:
            string += c
    return result

# replace=,(\s):\\1&&\":&&,\"\":\",\"&&\"\":\"&&\":\s




###########################################################
## Parser
############################################################


def parse_file(input_filename,attribute_value):


    # Some CSV files can not be loaded because the quotations are not set correctly
    # first correct them and  save in a new csv file
    if correct_quotation == 'yes':
        o = open(new_filename, 'w', newline='')
        writer = csv.writer(o, delimiter=',', quotechar='"')
        with open(input_filename, 'r') as f:
            reader = csv.reader(f, delimiter='|', quotechar='"')
            for row in reader:
                splitted_data = splitstring(row[0], ',', '"')
                writer.writerow(splitted_data)
        print("check the new file with the correct quotation")
        sys.exit()
        
        
    print(config)
    
    # if CSV file c
    if file_type == "csv":
        try:
            seperator = config['SHEET_1']['seperator']
        except:
            seperator =","
        try:
            skip_rows = config['SHEET_1']['skip_rows'].split(",")
        except:
            skip_rows = str()        
        print(skip_rows)    
        df_collection[0] = pd.read_csv(input_filename, quotechar='"',sep=seperator,engine='python',skiprows=int(skip_rows[1]))
        sheet_names = ['Sheet1']
    else:
        sheet_names = config['INITIAL']['sheet_names'].split(",")

    count = 0
    for sheet in sheet_names:
        sheet_name = "SHEET_" + str(count + 1)
        try:
            skip_rows = config[sheet_name]['skip_rows'].split(",")
        except:
            skip_rows = str()

        print(skip_rows)

        try:
            usecols = config[sheet_name]['usecols']
        except:
            usecols =""


        dropna = config[sheet_name]['dropna']
        if not file_type == "csv":
            if len(usecols) > 0:
                if len(skip_rows) > 1:
                    df_collection[count] = pd.read_excel(input_filename, sheet_name=sheet, usecols=usecols,
                                                         skiprows=range(int(skip_rows[0]), int(skip_rows[1])))
                else:
                    df_collection[count] = pd.read_excel(input_filename, sheet_name=sheet, usecols=usecols)
            else:
                if len(skip_rows) > 1:
                    df_collection[count] = pd.read_excel(input_filename, sheet_name=sheet,
                                                         skiprows=range(int(skip_rows[0]), int(skip_rows[1])))
                else:
                    df_collection[count] = pd.read_excel(input_filename, sheet_name=sheet)

            print (df_collection[0])


        try:
            select_columns = config[sheet_name]['select_columns'].split(",")
        except:
            select_columns=[]
        select_columns_quoted = ''
        if ((len(select_columns) > 0) and (len(select_columns[0]) > 0)) :
            i = 0
            for col in select_columns:
                if i > 0:
                    select_columns_quoted = select_columns_quoted + ","
                select_columns_quoted = select_columns_quoted + "'" + col + "'"
                i = i + 1
            eval_str = "df_collection[" + str(count) + "][[" + select_columns_quoted + "]]"
            df_collection[count] = eval(eval_str)

        try:
            chained_filter_condition = config[sheet_name]['chained_filter_condition'].split(",")
        except:
            chained_filter_condition = []

        if (len(chained_filter_condition)>0):
            for condition in chained_filter_condition:
                if (len(condition) > 0):
                    eval_condition = "df_collection[" + str(count) + "][(df_collection[" + str(count) + "]." + condition + ")]"
                    df_collection[count] = eval(eval_condition)
        if dropna == "yes":
            df_collection[count] = df_collection[count].dropna(how='all', axis=1)



        try:
            pre_melt_id_vars = config[sheet_name]['pre_melt_id_vars'].split(",")
            pre_melt_value_vars = config[sheet_name]['pre_melt_value_vars'].split(",")
            pre_melt_var_name = config[sheet_name]['pre_melt_var_name']
            pre_melt_value_name = config[sheet_name]['pre_melt_value_name']
        except:
            pre_melt_var_name=""

        if len(pre_melt_var_name) > 0:
            df_collection[0] = pd.melt(df_collection[0], id_vars=pre_melt_id_vars,
                                       value_vars=pre_melt_value_vars,
                                       var_name=pre_melt_var_name,
                                       value_name=pre_melt_value_name)
        ## attribute_value delivered in a cell before header rows
        if len(attribute_value)>0:
            df_collection[count]["attribute"] = attribute_value
        else:
            eval_components = config[sheet_name]['attribute_formula'].split("+")
            eval_result = eval_formula(eval_components, count, sheet)
            try:
                # if necessary evaluate
                df_collection[count]["attribute"] = eval(eval_result)
            except:
                # otherwise set the return value
                df_collection[count]["attribute"] = eval_result

        try:
            drop_columns = config[sheet_name]['drop_columns'].split(",")
        except:
            drop_columns=[]

        if len(drop_columns[0]) > 0:
            df_collection[count].drop(drop_columns, axis='columns', inplace=True)



        rename_columns = {}
        try:
            rename_columns = config[sheet_name]['rename_columns'].split(",")
        except:
            rename_columns = {}

        if len(rename_columns[0]) > 0:
            for i in rename_columns:
                rename_pair = i.split(sep=':')
                column_name = rename_pair[0]
                column_name_new = rename_pair[1]
                # rename columns according to the target
                df_collection[count].rename({column_name: column_name_new}, axis=1, inplace=True)

        count = count + 1

    id_vars = config['MELT']['id_vars'].split(",")
    var_name = config['MELT']['var_name']

    # APPEND

    for key in df_collection.keys():
        print(len(df_collection[key].index))
        if (key!=0):
            df_collection[0]=pd.DataFrame.append(df_collection[0],df_collection[key], ignore_index=True)

    print(len(df_collection[0].index))
    if len(var_name) > 0:
        df_collection[0] = pd.melt(df_collection[0], id_vars=id_vars,
                                   var_name=var_name,
                                   value_name='value')


    columns = list(df_collection[0])
    print(columns)

    # Add additional columns needed for target table
    df_collection[0]['id'] = np.nan
    df_collection[0]['dml_user'] = 'G804103'
    df_collection[0]['dml_timestamp'] = np.nan
    df_collection[0]['rec_source'] = os.path.basename(input_filename)
    df_collection[0]['alos_id'] = 20  # ???
    df_collection[0]['tech_start'] = np.nan
    df_collection[0]['tech_end'] = np.nan
    df_collection[0]['data_provider'] = data_provider
    df_collection[0]['valid_from'] = '01.01.1900'
    df_collection[0]['valid_until'] = '31.12.9999'
    if "country_iso_code" not in columns:
        df_collection[0]['country_iso_code'] = ''

    if "year" in columns:
        df_collection[0]['validity_date'] = df_collection[0]['year']
    else:
        df_collection[0]['validity_date'] = np.nan

    drop_columns = config['FINAL']['drop_columns'].split(",")
    if len(drop_columns[0]) > 0:
        df_collection[0].drop(drop_columns, axis=1, inplace=True)

    try:
        remove_non_numeric_values=config['FINAL']['remove_non_numeric_values']
    except:
        remove_non_numeric_values='no';
    if (remove_non_numeric_values=='yes'):
        df_collection[0]=df_collection[0][pd.to_numeric(df_collection[0]['value'], errors='coerce').notna()]

    columns = list(df_collection[0])
    print(columns)

    # Reorder the columns to the structure of target table
    column_names = ["id", "dml_user", "dml_timestamp", "rec_source", "alos_id", "tech_start", "tech_end",
                    "data_provider",
                    "country_name", "country_iso_code", "validity_date", "attribute", "value", "valid_from",
                    "valid_until"]
    df_collection[0] = df_collection[0].reindex(columns=column_names)
    # write into CSV file

    df_collection[0].to_csv(output_filename)
    return df_collection[0]


##############################################################################################
#### Start Main Procedure
##############################################################################################

my_parser = argparse.ArgumentParser(allow_abbrev=False)
my_parser.add_argument('--config', action='store', type=str, required=True)
my_parser.add_argument('--output', action='store', type=str, required=True)
my_parser.add_argument('--env', action='store', type=str, required=False)
my_parser.add_argument('--pw', action='store', type=str, required=False)
my_parser.add_argument('--load_to_db', action='store', type=str, required=False, default='no')
my_parser.add_argument('--batchsize', action='store', type=int, required=False, default=5000)
my_parser.add_argument('--correct_quotation', action='store', type=str, required=False, default='no')
my_parser.add_argument('--new_filename', action='store', type=str, required=False)

args = my_parser.parse_args()

config_filename = args.config
output_filename = args.output
env = args.env
pw = args.pw
i_batchsize = args.batchsize
load_to_db = args.load_to_db
correct_quotation = args.correct_quotation
new_filename = args.new_filename

if load_to_db == 'yes':
    if env == 'E':
        conn = create_engine(
            'oracle+cx_oracle://dm_esg:' + pw + '@dm19-e2-scan.srv.allianz/?service_name=S_IDSDE.srv.allianz').raw_connection()
    elif env == 'S':
        conn = create_engine(
            'oracle+cx_oracle://dm_esg:' + pw + '@dm70-e2-scan.srv.allianz/?service_name=S_IDSDS.srv.allianz').raw_connection()

config = configparser.ConfigParser()

print(config_filename)

config.read(config_filename)

try:
    input_directory = config['INITIAL']['input_directory']
except:
    input_directory = str()

try:
    attribute_value_stored_in_cell = config['INITIAL']['attribute_value_stored_in_cell']
except:
    attribute_value_stored_in_cell = str()

data_provider = config['INITIAL']['data_provider']
file_list = list()
if len(input_directory) > 0:
    for file in os.listdir(input_directory):
        file_list.append(os.path.join(input_directory, os.fsdecode(file)))
else:
    file_list.append(config['INITIAL']['input_filename'])

print("file_list:")
print(file_list)

k = 0
for input_filename in file_list:
    print(input_filename)
    if input_filename.endswith(".xls") or input_filename.endswith(".xlsx") or input_filename.endswith(".csv"):
        print("2")
        # file_type = input_filename.split(".")[1]
        # file_name = input_filename.split(".")[0]
        filename_length = len(input_filename)
        file_type = input_filename[filename_length - 3:filename_length]
        file_name = input_filename[0:filename_length - 4]
        print(file_type)
        print(file_name)

        if (len(attribute_value_stored_in_cell) > 0):
            wb = openpyxl.load_workbook(input_filename)
            sheet = wb.active
            attribute_value = sheet['' + attribute_value_stored_in_cell + ''].value
        else:
            attribute_value = str()

        df_collection = {}
        parse_file(input_filename, attribute_value)

        print("load_to_db:" + load_to_db)

        #if load_to_db == "yes":
            #load_to_database()
    else:
        print("3")
        continue

    k = k + 1





    












# https://blogs.oracle.com/opal/efficient-and-scalable-batch-statement-execution-in-python-cx_oracle


