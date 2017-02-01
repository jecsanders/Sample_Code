# Written in Python 3.4 on Windows 10 by Connor Sanders
# Development V. 0.4


import os
import csv
import json
import sys
import datetime
import traceback
import shutil
import pip
try:
    import pypyodbc
except:
    pip.main(['install', 'pypyodbc'])
    import pypyodbc

# Declare configuration variables
sql_query_dir = os.getcwd() + '\\sql_files\\queries\\'
traceback_str = ''

# Set up local MS SQL ODBC database connection string and build driver variable
user = 'username'
password = 'password'
database = 'db_name'
server = 'server'
port = 1433
driver = "DRIVER={SQL Server};SERVER=" + server + ";PORT=" + str(port) + ";UID=" + user + ";PWD=" + password +\
         ";DATABASE=" + database

# Set up local access database file path and create mdb access variable
raw_default_dir = os.getcwd().replace('\\', '/')
split_default_dir = raw_default_dir.split('/')
len_db_dir = len(split_default_dir)
base_dir = str(split_default_dir[0:len_db_dir - 1]).replace("['", "").replace("']", "").replace("', '", "/")
weekday_dict = {'monday': 'mon', 'tuesday': 'tue', 'wednesday': 'wed', 'thursday': 'thu', 'friday': 'fri',
                'saturday': 'sat', 'sunday': 'sun'}
filter_param_dict = {'greater equal': '>=', 'less equal': '<=', 'greater': '>', 'less': '<', 'equal': '==', 'is':'is'}
default_key_field_pair = [{'Run': ['Run_Number'], 'Sequence': ['Sequence_Number']}]

# Connect to Database and create cursor
conn = pypyodbc.connect(driver)


# Function to get current time
def get_time_str():
    date_time = str(datetime.datetime.now())
    cur_hour = date_time.split(' ')[1].split('.')[0].split(':')[0]
    cur_min = date_time.split(' ')[1].split('.')[0].split(':')[1]
    cur_time = cur_hour + ':' + cur_min
    return cur_time


# Function to determine correct format for day of week
def determine_weekday(user_input):
    for k, v in weekday_dict.items():
        if user_input.lower() == k or user_input.lower() == v:
            return k


# Function to get report dict from local parameter file
def get_report_dict():
    report_file = os.getcwd() + '\\config\\report_list.txt'
    o_report_file = open(report_file, 'r')
    report_dict = {}
    for report in o_report_file:
        for report_obj in report.split(','):
            dict_ent = {report_obj.split(':')[0]: report_obj.split(':')[1]}
            report_dict.update(dict_ent)
    o_report_file.close()
    return report_dict.items()


# Function to modify report list parameter file
def modify_report_file(mod_type, report_name, output_type, sql_file, schedule_type='none', day_of_month='none',
                       day_of_week='none', scheduled_run_time='none'):

    # Get dict of saved reports and built in memory data list of report names
    report_file = os.getcwd() + '\\config\\report_list.txt'
    c = 0
    new_data_list = []
    for k, v in get_report_dict():
        if k == report_name + ' | ' + output_type + ' | ' + schedule_type + ' | ' + day_of_month + ' | '\
                   + day_of_week + ' | ' + scheduled_run_time:
            c += 1
        new_data_list.append(k + ':' + v)

    # Functionality to add a report
    if mod_type == 'add' and c == 0:
        o_report_file = open(report_file, 'w')
        new_data = report_name + ' | ' + output_type + ' | ' + schedule_type + ' | ' + day_of_month + ' | ' \
                   + day_of_week + ' | ' + scheduled_run_time + ':' + sql_file
        new_data_list.append(new_data)
        o_report_file.write(str(new_data_list).replace("['", "").replace("']", "").replace("', '", ","))
        o_report_file.close()
        print(report_name + ' has been ' + mod_type + 'ed and is based on ' + sql_file)

    # Functionality to delete a report
    elif mod_type == 'delete' and c >= 1:
        o_report_file = open(report_file, 'w')
        new_data_list.remove(report_name + ' | ' + output_type + ' | ' + schedule_type + ' | ' + day_of_month + ' | '
                             + day_of_week + ' | ' + scheduled_run_time + ':' + sql_file)
        str(new_data_list).replace("['", "").replace("']", "").replace("', '", ",")
        o_report_file.write(str(new_data_list).replace("['", "").replace("']", "").replace("', '", ","))
        o_report_file.close()
        print(report_name + ' has been ' + mod_type + 'd')

    # Functionality to edit a report
    if mod_type == 'edit':
        o_report_file = open(report_file, 'w')
        for row in new_data_list:
            if report_name in row:
                new_data_list.remove(row)
        new_data = report_name + ' | ' + output_type + ' | ' + schedule_type + ' | ' + day_of_month + ' | ' \
                   + day_of_week + ' | ' + scheduled_run_time + ':' + sql_file
        new_data_list.append(new_data)
        o_report_file.write(str(new_data_list).replace("['", "").replace("']", "").replace("', '", ","))
        o_report_file.close()
        print(report_name + ' has been ' + mod_type + 'ted Successfully!')

    return


# Function to return field combinations to check for unique pairs by
def field_combinations(iterable, r):
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(list(range(r))):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in list(range(i+1, r)):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)


# Function to determine primary key sets from returned data
def determine_primary_keys(in_table):
    prim_query = 'SELECT * FROM ' + in_table + ';'
    prime_key_dict = {}
    prim_cursor = conn.cursor()
    prim_cursor.execute(prim_query)
    re_data = prim_cursor.fetchall()
    columns = [column[0] for column in prim_cursor.description]
    row_list = []
    p_key_count = 0
    multi_key_list = []

    # If table has more than 1k records, attempt to determine unique id
    if len(re_data) > 1000:
        for row in re_data:
            i = 0
            re_data_dict = {}
            for r in row:
                dict_ent = {columns[i]: r}
                re_data_dict.update(dict_ent)
                i += 1
            row_list.append(re_data_dict)

        # Iterate throguh column list and check for unique fields
        for column in columns:
            column_data_list = []
            for data in row_list:
                column_data_list.append(data[column])
            returned_list_count = len(column_data_list)
            no_duplicates_count = len(set(column_data_list))

            # If Unique field found print to console and add to unique field dict
            if no_duplicates_count == returned_list_count:
                print(str(column) + ' is the unique table ID.')
                dict_ent = {column: in_table}
                prime_key_dict.update(dict_ent)
                p_key_count += 1

            # Else add them to a list to be checked for multi field unique identifiers
            elif no_duplicates_count != 1 and None not in set(column_data_list):
                multi_key_list.append(column)
            else:
                pass

    # If table has less than 1k records, attempt to match returned columns to commonly used unique ids
    else:
        default_counter = 0
        key_columns = []
        for default_field_dict in default_key_field_pair:
            for k, v in default_field_dict.items():
                if k.lower() in columns:
                    key_columns.append(k.lower())
                    default_counter += 1
                elif k.lower() not in columns:
                    for alias_field in v:
                        if alias_field.lower() in columns:
                            key_columns.append(alias_field.lower())
                            default_counter += 1
        if default_counter == 2:
            for dkf in key_columns:
                dict_ent = {dkf: in_table}
                prime_key_dict.update(dict_ent)
                p_key_count += 1
        else:
            print('Not enough records returned to determine an accurate unique record identifier.')
            dict_ent = {'Not enough data': in_table}
            prime_key_dict.update(dict_ent)
            p_key_count += 1

    # If no unique fields found in data, check for multi field identifier
    if p_key_count == 0:
        key_count = 2
        while True:
            listCombined = field_combinations(multi_key_list, key_count)
            for field_combo in listCombined:
                list_len = len(field_combo)
                check_pair_list = []
                for data in row_list:
                    i = 0
                    re_list = []
                    while i < list_len:
                        it_field = field_combo[i]
                        list_entry = str(data[it_field])
                        re_list.append(list_entry)
                        i += 1
                    check_pair_list.append(str(re_list))
                returned_list_count = len(check_pair_list)
                no_duplicates_count = len(set(check_pair_list))

                # If Unique field found print to console and add to unique field dict
                if no_duplicates_count == returned_list_count:
                    print(str(field_combo) + ' is a unique pair!')
                    dict_ent = {str(field_combo): in_table}
                    prime_key_dict.update(dict_ent)
                    p_key_count += 1
                    break
                else:
                    pass
            if p_key_count == 0:
                key_count += 1
            else:
                break

    prim_cursor.close()
    return prime_key_dict.items()


# Function to exit system after a successful action or error
def exit_system_prompt(err=False):

    # Print message depending on whether there is an error
    if err is True:
        print('There was an error with your request, Would you like to try again?')
    else:
        print('Action was Successful!')
        print('Would you like to run or manage another report or query?')

    # Prompt user to exit or restart. Loop until the user selects a correct option
    while True:
        exit_re = input('   Enter Y/N: ')
        if exit_re.lower() == 'y' or exit_re.lower() == 'yes':
            break
        elif exit_re.lower() == 'n' or exit_re.lower() == 'no':
            print('Exiting program...')
            conn.close()
            sys.exit(0)
        else:
            print('User input not recognized\n Please try again....')

    return True


# Function to prompt user to confirm exit or restart when user inputs a quit or restart command
def restart_exit_prompt(input_type, add_report='', del_report=''):
    status = ''

    # Prompt user to confirm exit/restart choice. Loop until a correct input is given
    while True:
        print('Are you sure you want to ' + input_type + ' the system?')
        ex_input = input('   ' + input_type.title() + '? Enter Y/N: ')
        if ex_input.lower() == 'y' or ex_input.lower() == 'yes':
            print(input_type.title() + 'ing program...')
            if input_type == 'exit':
                if add_report != '':
                    shutil.rmtree(add_report)
                if del_report != '':
                    os.makedirs(del_report)
                conn.close()
                sys.exit(0)
            elif input_type == 'restart':
                status = input_type
                break

        # If user changes mind, or inputted an exit or restart command by mistake then bring user back to previous step
        elif ex_input.lower() == 'n' or ex_input.lower() == 'no':
                status = 'reinput'
                break
        else:
            print('User input not recognized... Try again')

    return status


# Function to prompt user to exit or restart script
def exit_user_prompt(input_gen, add_report='', del_report=''):

    # Create exit and restart lists with user input options
    exit_list = ['e', 'exit', 'q', 'quit']
    restart_list = ['r', 're', 'restart']
    status = 'continue'

    # Check if user input matches what is in the exit and restart lists
    if input_gen.lower() in exit_list:
        status = restart_exit_prompt('exit', add_report, del_report)
    elif input_gen.lower() in restart_list:
        status = restart_exit_prompt('restart')

    return status


# Function to build dict of all tables with fields and data types in database
def database_model_dict():

    # Open two cursors to handle the table request and the column request
    n_cursor = conn.cursor()
    tbl_dict = {}
    tbl_list = []

    # Iterate through returned table list from database then iterate through all availble columns from each table
    for rows in n_cursor.tables():
        if rows[3] == 'TABLE':
            tablename = rows[2]
            tbl_list.append(tablename)
    for tbl in tbl_list:
        fld_dict = {}
        for fld in n_cursor.columns(tbl):
            fld_ent = {fld[3]: fld[5]}
            fld_dict.update(fld_ent)
        tbl_ent = {tbl: fld_dict}
        tbl_dict.update(tbl_ent)
    n_cursor.close()

    return tbl_dict.items()


# SQL Statement Constructor
def create_sql_statement(stmt_type, table, sel_fields, limit=0, filter_string='', unique_field_string='',
                         join=' INNER JOIN '):
    sql = ''
    table_str = str(table).replace("['", "").replace("['", "").replace("', '", ",")
    if stmt_type == 'report_query' or stmt_type == 'query':

        # SQL Statement constructor for a single table query
        if ',' not in table_str:
            base_sql = str(sel_fields) + ' FROM ' + str(table)

            # Single table query with no filter
            if filter_string.lower() == 'none' or filter_string.lower() == 'no' or filter_string.lower() == 'n'\
                    or filter_string.lower() == '':
                if limit > 0:
                    sql = 'SELECT TOP ' + str(limit) + ' ' + base_sql + ';'
                else:
                    sql = 'SELECT ' + base_sql + ';'

            # Single table query with filter
            else:
                in_filter = filter_string.replace('"]', '').replace('["', '')
                if limit > 0:
                    sql = 'SELECT TOP ' + str(limit) + ' ' + base_sql + ' WHERE ' + in_filter + ';'
                else:
                    sql = 'SELECT ' + base_sql + ' WHERE ' + in_filter + ';'

        # SQL Statement constructor for a multi table query
        else:
            len_table_list = len(table)
            join_field_list = []
            if ',' in unique_field_string:
                split_unique_field_string = unique_field_string.split(',')
                for in_unique_field in split_unique_field_string:
                    join_field_list.append(in_unique_field)
            else:
                join_field_list.append(unique_field_string)
            sql_string_list = []
            if '=' not in unique_field_string:
                if len_table_list == 3:
                    base_sql = table[0] + '.' + unique_field_string.replace(',', str(', ' + table[0] + '.')) + ', ' + \
                               sel_fields.replace(',', ', ') + ' FROM ' + '(' + table[0]
                else:
                    base_sql = table[0] + '.' + unique_field_string.replace(',', str(', ' + table[0] + '.')) + ', ' + \
                               sel_fields.replace(',', ', ') + ' FROM ' + table[0]
            else:
                primary_table_join_fields = []
                for join_fields in unique_field_string.split(','):
                    prim_field = join_fields.split('=')[0]
                    primary_table_join_fields.append(prim_field)
                if len_table_list == 3:
                    base_sql = str(primary_table_join_fields).replace("']", ", ").replace("['", "").replace("', '", ", ")\
                               + ', ' + sel_fields.replace(',', ', ') + ' FROM ' + '(' + table[0]
                else:
                    base_sql = str(primary_table_join_fields).replace("']", ", ").replace("['", "").replace("', '", ", ")\
                               + sel_fields.replace(',', ', ') + ' FROM ' + table[0]

            # If user opted for no filter
            if filter_string.lower() == 'none' or filter_string.lower() == 'no' or filter_string.lower() == 'n' or \
                                        filter_string.lower() == '':

                # Query with multiple tables joined and limit with no filter
                if limit > 0:
                    in_sql = 'SELECT TOP ' + str(limit) + ' ' + base_sql
                    sql_string_list.append(in_sql)
                    i = 0
                    while i < len_table_list - 1:

                        # Builder join clause for first join in triple join
                        if i == 0 and len_table_list == 3:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

                        # Builder join clause for later joins in triple join, or first and only join in single join
                        elif i < len_table_list - 1:

                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field
                                        add_str += join_field_str
                                        add_str += ';'
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1]
                                        add_str += join_field_str
                                        add_str += ';'
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

                # Query with multiple tables joined with no limit and no filter
                else:
                    in_sql = 'SELECT ' + base_sql
                    sql_string_list.append(in_sql)
                    i = 0
                    while i < len_table_list - 1:

                        # Builder join clause for first join in triple join
                        if i == 0 and len_table_list == 3:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                        join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                        join_field + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

                        # Builder join clause for later joins in triple join, or first and only join in single join
                        elif i < len_table_list - 1:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field
                                        add_str += join_field_str
                                        add_str += ';'
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1]
                                        add_str += join_field_str
                                        add_str += ';'
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

            # Query with multiple tables joined and limit with filter
            else:
                if limit > 0:
                    in_sql = 'SELECT TOP ' + str(limit) + ' ' + base_sql
                    sql_string_list.append(in_sql)
                    i = 0
                    while i < len_table_list - 1:

                        # Builder join clause for first join in triple join
                        if i == 0 and len_table_list == 3:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                        join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                        join_field + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

                        # Builder join clause for later joins in triple join, or first and only join in single join
                        elif i < len_table_list - 1:

                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If last iteration add filter and break out of loop constructor
                                if i == len_table_list - 1:
                                    #add_str += ' WHERE ' + str(filter_string) + ';'
                                    #sql_string_list.append(add_str)
                                    break

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field
                                        add_str += join_field_str
                                        add_str += ' WHERE ' + str(filter_string) + ';'
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1]
                                        add_str += join_field_str
                                        add_str += ' WHERE ' + str(filter_string) + ';'
                                        jf_count += 1

                            sql_string_list.append(add_str)
                            i += 1

                # Query with multiple tables joined with filter and no limit
                else:
                    in_sql = 'SELECT ' + base_sql
                    sql_string_list.append(in_sql)
                    i = 0
                    while i < len_table_list - 1:

                        # Builder join clause for first join in triple join
                        if i == 0 and len_table_list == 3:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ')'
                                        add_str += join_field_str
                                        jf_count += 1

                            sql_string_list.append(add_str)

                        # Builder join clause for later joins in triple join, or first and only join in single join
                        elif i < len_table_list - 1:
                            add_str = join + table[i + 1] + ' ON '
                            jf_count = 0
                            jf_len = len(join_field_list)
                            for join_field in join_field_list:

                                # If last iteration add filter and break out of loop constructor
                                if i == len_table_list - 1:
                                    break

                                # If user enters field names to join by that match
                                if '=' not in join_field:
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = table[i] + '.' + join_field + ' = ' + table[i + 1] + '.' + \
                                                         join_field
                                        add_str += join_field_str
                                        add_str += ' WHERE ' + str(filter_string) + ';'
                                        jf_count += 1

                                # If user enters field names to join by that do not match
                                else:
                                    s_join_field = join_field.split('=')
                                    if jf_len > 1 and jf_count < jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1] + ' AND '
                                        add_str += join_field_str
                                        jf_count += 1
                                    elif jf_count == jf_len - 1:
                                        join_field_str = s_join_field[i] + ' = ' + s_join_field[i + 1]
                                        add_str += join_field_str
                                        add_str += ' WHERE ' + str(filter_string) + ';'
                                        jf_count += 1

                            sql_string_list.append(add_str)
                        i += 1

            # Create SQL Statement String from generated list
            sql = str(sql_string_list).replace("['", "").replace("']", "").replace("', '", "").replace('["', '')\
                .replace('"]', '').replace('", "', "").replace("', ", "").replace('"', '').replace(' , ', ' ')
            #print(sql)

    return sql


# Function to create and execute SQL Statements
def execute_sql_stmt(stmt_type, table, sel_fields, saved_sql_name='', limit=0, save_file=False, overwrite=True,
                     filter_string='none', unique_field_string='', join=' INNER JOIN '):

    # Run create_sql_statement function with user inputted parameters to create SQL statement to execute
    sql = create_sql_statement(stmt_type, table, sel_fields, limit, filter_string=filter_string,
                               unique_field_string=unique_field_string, join=join)

    # If SQL Statement will be run for a report
    if stmt_type == 'report_query':
        qry_cursor = conn.cursor()
        returned_data = []
        try:
            returned_qry = qry_cursor.execute(sql)
            c = 0

            for row in returned_qry:
                returned_data.append(row)
                c += 1
            qry_cursor.close()
            return returned_data
        except:
            qry_cursor.close()
            return ['None']

    # If SQL Statement will be run off a manual SQL query or saved SQL file
    elif stmt_type == 'query':
        try:
            cur_dir = os.getcwd()
            out_dir = cur_dir + '\\reports\\sql_queries\\'
            execute_sql_to_file(sql, out_dir, 'sql')
            if save_file:
                save_sql_query(sql_query_dir, sql, saved_sql_name, overwrite=overwrite)
                return ['success']
            else:
                return ['success']
        except:
            return ['Error']


# SQL Query Function
def sql_query(run_type, table, fields=['*'], sql_file_name='', limit=0, add_editor=False, overwrite=True,
              filter_string='none', unique_field_string='', join=' INNER JOIN '):

    # Run a select * statement to grab all available columns from selected table
    if '*' in fields:
        sel_fields = fields[0]
        ex_sql = execute_sql_stmt(run_type, table, sel_fields, sql_file_name, limit, add_editor, overwrite,
                                  filter_string, unique_field_string, join)

    # Create sel_fields string and as an optional parameter in run execute_sql_stmt
    else:
        sel_fields = str(fields).replace("['", "").replace("']", "").replace("', '", ",")
        ex_sql = execute_sql_stmt(run_type, table, sel_fields, sql_file_name, limit, add_editor, overwrite,
                                  filter_string, unique_field_string, join)

    return ex_sql


# Function to determine which tables have data
def check_table_dict():
    checked_table_dict = {}

    # Iterate through database model dictionary and check for tables with no data to exclude from checked_table_dict
    for t, f in database_model_dict():
        row_list = []
        for r in sql_query('report_query', t):
            if r != 'None':
                row_list.append(r)
                checked_dict_ent = {t: [len(row_list), f]}
                checked_table_dict.update(checked_dict_ent)

    return checked_table_dict.items()


# Function to correctly format returned query data to use in csv writer function
def build_data_dict(qry_results, columns):
    row_list = []
    for row in qry_results:
        c = 0
        row_dict = {}

        # Iterate through records within returned rows to fix/consistently format data
        for r in row:
            sel_column = columns[c]

            # Fix/format date stamps
            if 'date' in sel_column.lower():
                n_date = str(r).split(' ')[0]
                row_ent = {sel_column: n_date}
                row_dict.update(row_ent)

            # Fix/format time stamps
            elif 'time' in sel_column.lower():
                n_time = str(r).split(' ')[1]
                row_ent = {sel_column: n_time}
                row_dict.update(row_ent)

            # Format rest of returned data
            else:
                row_ent = {sel_column: str(r).replace('\n', '')}
                row_dict.update(row_ent)
            c += 1
        row_list.append(row_dict)

    return row_list


# Function to write JSON File
def write_to_json_file(json_file, dict_data):
    try:
        with open(json_file, 'w') as jsonfile:
            json.dump(dict_data, jsonfile, indent=4)
        jsonfile.close()
    except:
        print('Errr writing to JSON file')


# Function to write dat dict to csv format
def write_to_csv_file(csv_file, csv_columns, dict_data):
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
        csvfile.close()
    except:
        print('Error writing to csv file!')
        sys.exit(1)

    return


# Function to check for extension in file name
def check_for_extension(in_file_name, ex_ext):

    # If extension found in entered file
    if '.' in in_file_name:
        s_in_file = in_file_name.split('.')
        len_s_in_file = len(s_in_file)
        if len_s_in_file == 2:
            ext = s_in_file[1]

            # Ensure user inputted file extension matches what the script is expecting
            if ext != ex_ext:
                if ex_ext == 'sql':
                    print('Entered ' + ex_ext.upper() + ' files you want to run must end with a .' + ex_ext +
                          ' extension!')
                    print('You can also leave off the .' + ex_ext + ' if saved sql file in directory ends with .sql')
                else:
                    print('Incorrect file type entered! Please ensure your inputted file ends in a .' + ex_ext +
                          ' extension and try again!')
                    print('You can also leave the extension off for the system default of csv format!')
                sys.exit(0)
            else:
                return 'ext inputted'
        else:
            print('Incorrect file format entered!')
            sys.exit(0)

    # If no extension entered, return extension script is expecting
    else:
        return ex_ext


# Function to determine user choice
def user_input(user_string, add_report='', del_report=''):
    status = ''
    user_res = ''

    # Loop until user inputs either restart, exit, or enters a valid input command for the specified step
    while True:
        user_res = input(user_string)
        exit_return = exit_user_prompt(user_res, add_report, del_report)
        if exit_return == 'restart':
            status = 'restart'
            break
        elif exit_return == 'reinput':
            pass
        elif exit_return == 'continue':
            status = 'continue'
            break

    return status + '|' + user_res


# Function to return correct file name to run based on extension
def determine_filename(file_dir, sel_file, ex_ent):

    # Check if file extension was correctly entered by user
    entered_ext = check_for_extension(sel_file, ex_ent)
    if entered_ext == 'ext inputted':
        sql_file = file_dir + sel_file
    else:
        sql_file = file_dir + sel_file + '.' + entered_ext

    return sql_file


# Function to read SQL file
def read_sql_file(sql_dir, sql_file):

    # Determine correct SQL file name from entered user input and return read file string
    check_sql_file = determine_filename(sql_dir, sql_file, 'sql')
    o_sql_file = open(check_sql_file, 'r')
    in_sql = " ".join(o_sql_file.readlines())
    o_sql_file.close()

    return in_sql


# function to execute user sql and return data to .csv file
def execute_sql_to_file(in_sql, tar_dir, exe_type, result_file='', output_type='csv'):
    status = 'success'
    result_file_name = result_file

    # Open connection cursor and execute sql code
    user_cursor = conn.cursor()
    try:
        user_cursor.execute(in_sql)
        qry_results = user_cursor.fetchall()
        columns = [column[0] for column in user_cursor.description]
        print('Query Succeeded!')
    except:
        print('Query failed!')
        user_cursor.close()
        status = 'error'
        return status

    # Prompt user for .csv file name to save results to if selected execute type is a one off SQL statement from user
    o_type = output_type
    if exe_type == 'sql':
        print('Enter the desired filename ending in a .csv or .json extension to save results to. If no extension is'
              ' entered, the default is .csv')
        exit_return = user_input('   Enter Filename: ')
        if exit_return.split('|')[0] == 'restart':
            status = 'restart'
        elif exit_return.split('|')[0] == 'continue':
            result_file_name = exit_return.split('|')[1]
            if '.' in result_file_name:
                split_o_file = result_file_name.split('.')
                len_split_o_file = len(split_o_file)
                output_type = split_o_file[len_split_o_file - 1].lower()
                o_type = output_type

    # Iterate through query results and write corrected data rows to csv file
    try:
        row_list = build_data_dict(qry_results, columns)
        o_file_name = determine_filename(tar_dir, result_file_name, o_type)
        split_o_file = o_file_name.split('.')
        len_split_o_file = len(split_o_file)
        deter_o_type = split_o_file[len_split_o_file - 1].lower()
        if deter_o_type.lower() == 'csv':
            write_to_csv_file(o_file_name, columns, row_list)
            print(result_file_name + '.' + deter_o_type + ' has been created and can be found in ' + tar_dir + '!')
        elif deter_o_type.lower() == 'json':
            write_to_json_file(o_file_name, row_list)
            print(result_file_name + ' has been created and can be found in ' + tar_dir + '!')
        user_cursor.close()
    except:
        print('Saving data to file failed!')
        user_cursor.close()
        status = 'error'

    return status


# Function to check new sql file name and kick off modify_saved_sql function to create new SQL file
def create_saved_sql_file(sql_file_dir, sql_file, in_sql, add_editor=False, overwrite=True):

    # Check user inputted name to ensure correct extension is included in coming open file statement
    status = 'success'
    checked_added_sql_name = determine_filename(sql_file_dir, sql_file, 'sql')
    checked_file_name = checked_added_sql_name.split('\\')
    len_checked_file = len(checked_file_name)
    final_sql_file = checked_file_name[len_checked_file - 1]
    if add_editor:
        status = modify_saved_sql('add', final_sql_file, in_sql, add_editor=add_editor, overwrite=overwrite)
    elif add_editor is False and overwrite:
        status = modify_saved_sql('add', final_sql_file, in_sql, add_editor=add_editor, overwrite=overwrite)
    elif add_editor is False:
        status = modify_saved_sql('add', final_sql_file, in_sql, add_editor=add_editor, overwrite=overwrite)

    return status


# Prompt user to save sql query to file
def save_sql_query(sql_file_dir, in_sql, sql_file_name='', overwrite=True):

    # If no parameter sql file name provided prompt user for sql file name
    status = 'success'
    if sql_file_name == '':
        print('Would you like to save entered query to a SQL File for future use?')
        save_query_re = ''
        exit_return = user_input('   Enter Y/N: ')
        if exit_return.split('|')[0] == 'restart':
            status = 'restart'
            return status
        elif exit_return.split('|')[0] == 'continue':
            save_query_re = exit_return.split('|')[1]

        # If user wants to save file
        sql_file = ''
        if save_query_re.lower() == 'y' or save_query_re.lower() == 'yes':
            print('Enter a filename for your new SQL file')
            exit_return = user_input('   Enter SQL filename: ')
            if exit_return.split('|')[0] == 'restart':
                status = 'restart'
                return status
            elif exit_return.split('|')[0] == 'continue':
                sql_file = exit_return.split('|')[1]
            status = create_saved_sql_file(sql_file_dir, sql_file, in_sql, True, overwrite)

    # If parameter sql file name provided
    else:
        sql_file = sql_file_name
        status = create_saved_sql_file(sql_file_dir, sql_file, in_sql, True, overwrite)

    return status


# Function to query database for user filter options
def get_filter_options(filter_field, selected_table):
    field_filter_options = []
    filter_cursor = conn.cursor()

    # If user enters multiple fields to filter by
    if ',' in str(selected_table):
        return_data_dict = {}
        filt_opt_count = 0
        for sel_table in selected_table:
            print(filter_field)
            print(sel_table)
            return_data_list = []
            filter_option_query = 'SELECT DISTINCT ' + filter_field + ' FROM ' + sel_table + ';'
            print(filter_option_query)
            try:
                filter_cursor.execute(filter_option_query)
                filter_option_re_data = filter_cursor.fetchall()
                for filter_option_data in filter_option_re_data:
                    re_data = filter_option_data[0]
                    if re_data != '':
                        return_data_list.append(re_data)
                if filt_opt_count == 0:
                    dict_ent = {sel_table:  return_data_list}
                    return_data_dict.update(dict_ent)
                    filt_opt_count += 1
                else:
                    for re_data in return_data_list:
                        for filt_field_list in return_data_dict.values():
                            if re_data in filt_field_list:
                                return_data_list.remove(re_data)
                    dict_ent = {sel_table:  return_data_list}
                    return_data_dict.update(dict_ent)
            except:
                print('No Data Returned')
        filter_cursor.close()
        for k, v in return_data_dict.items():
            for flt_flds in v:
                field_filter_options.append(flt_flds)

    # If user enters a single field to filter by
    else:
        filter_option_query = 'SELECT DISTINCT ' + filter_field + ' FROM ' + selected_table + ';'
        try:
            filter_cursor.execute(filter_option_query)
            filter_option_re_data = filter_cursor.fetchall()
            for filter_option_data in filter_option_re_data:
                re_data = filter_option_data[0]
                if re_data != '':
                    field_filter_options.append(re_data)
        except:
            print('No Data Returned')
        filter_cursor.close()

    return field_filter_options


# Function to determine filter value
def determine_filter_value(in_filter):
    re_filter = in_filter
    for k, v in filter_param_dict.items():
        if in_filter.lower() == k:
            re_filter = v
            break

    return re_filter


# Function to list available tables and field to assist query builder
def list_db_tables_fields(add_editor=False, sql_file_name='', overwrite=True):

    # List tables with data
    cur_dir = os.getcwd()
    selected_table = ''
    unique_field_name = ''
    sel_unique_ids = ''
    filter_string = ''
    primary_keys_dict = {}
    status = 'success'
    print('Processing newest model of tables with available data...')
    print('   This may take a moment...')
    print('Number     |     Table Name     |       Number of Records')
    table_dict = {}
    table_list = []
    in_mem_check_list = []
    t_count = 1
    check_tables_dict = check_table_dict()
    for t, f_list in check_tables_dict:
        table_list.append(t)
        dict_ent = {str(t_count): t}
        table_dict.update(dict_ent)
        print(str(t_count) + ' | ' + t + ' | ' + str(f_list[0]))
        t_count += 1
        for f, ft in f_list[1].items():
            check_ent = t + '|' + f + '|' + ft
            in_mem_check_list.append(check_ent)

    # Database Explorer while loop with built in SQL Builder functionlity
    i = 0
    while i == 0:
        sel_table = ''
        print('Select table whose columns you want to view by entering either the table name or associated list number')
        print('You can also enter No to continue to query builder')
        exit_return = user_input('   Enter Table Name/List Number/No: ')
        if exit_return.split('|')[0] == 'restart':
            status = 'restart'
            return status
        elif exit_return.split('|')[0] == 'continue':
            sel_table = exit_return.split('|')[1]

        # If user enters number instead of report name
        if sel_table.lower() == 'n' or sel_table.lower() == 'no':
            i += 1
            break
        else:
            if sel_table not in table_list:
                for k, v in table_dict.items():
                    if k == str(sel_table):
                        selected_table = v
            else:
                selected_table = sel_table

            # Print selected table's fields to console for user to choose from
            print("\n" + selected_table + "'s Fields  ")
            for k, v in determine_primary_keys(selected_table):
                print(str(k).replace("('", "").replace("')", "").replace("', '", ", ") + ' is the unique identifier')
            print('Number  |     Field     |      Data Type')
            f_count = 1
            ex_field_dict = {}
            ex_field_list = []
            for record in in_mem_check_list:
                split_record = record.split('|')
                select_table = split_record[0]
                if selected_table == select_table:
                    select_field = split_record[1]
                    select_field_type = split_record[2]
                    f_dict_ent = {str(f_count): select_field}
                    ex_field_dict.update(f_dict_ent)
                    ex_field_list.append(select_field)
                    print('   ' + str(f_count) + '   |   ' + select_field + '  |  ' + select_field_type)
                    f_count += 1

            # Prompt user go view different set of tables and columns
            view_another_table = ''
            print('Would you like to check out another set of tables and columns before continuing?')
            exit_return = user_input('   Enter Y/N: ')
            if exit_return.split('|')[0] == 'restart':
                status = 'restart'
                return status
            elif exit_return.split('|')[0] == 'continue':
                view_another_table = exit_return.split('|')[1]
            if view_another_table.lower() == 'y' or view_another_table.lower() == 'yes':
                pass
            elif view_another_table.lower() == 'n' or view_another_table.lower() == 'no':
                i += 1

    # Prompt user to run query builder or not
    query_assist = ''
    print('Do you want to run assisted query builder or manually enter SQL?')
    exit_return = user_input('   Enter Builder/SQL: ')
    if exit_return.split('|')[0] == 'restart':
        status = 'restart'
        return status
    elif exit_return.split('|')[0] == 'continue':
        query_assist = exit_return.split('|')[1]

    # If user wants to run assisted builder
    if query_assist.lower() == 'builder':

        # Builder main while loop
        x = 0
        r = 0
        while x == 0:

            # Prompt user to enter list of tables to query
            try:

                # If this is not the users first loop, reprint the table list to console
                if r == 1:
                    n_count = 1
                    print('\nNumber     |     Table Name     |       Number of Records')
                    for nt, nf_list in check_tables_dict:
                        print(str(n_count) + ' | ' + nt + ' | ' + str(nf_list[0]))
                        n_count += 1
                    print('')

                    # Builder sub explorer functionality main while loop
                    while True:

                        # Prompt user to select a table to view it's fields.
                        sel_table = ''
                        print('Select table whose columns you want to view by entering either the table name'
                              ' or associated list number. You can also enter No to continue to query builder')
                        exit_return = user_input('   Enter Table Name/List Number/No: ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            return status
                        elif exit_return.split('|')[0] == 'continue':
                            sel_table = exit_return.split('|')[1]

                        # If user enters number instead of report name
                        if sel_table.lower() == 'n' or sel_table.lower() == 'no':
                            break
                        else:
                            if sel_table not in table_list:
                                for k, v in table_dict.items():
                                    if k == str(sel_table):
                                        nselected_table = v
                            else:
                                nselected_table = sel_table

                            # Print selected table's fields to console for user to choose from
                            print("\n" + nselected_table + "'s Fields  ")
                            for k, v in determine_primary_keys(nselected_table):
                                print(str(k).replace("('", "").replace("')", "").replace("', '", ", ") +
                                      ' is the unique identifier')
                            print('Number  |     Field     |      Data Type')
                            nf_count = 1
                            for nrecord in in_mem_check_list:
                                nsplit_record = nrecord.split('|')
                                nselect_table = nsplit_record[0]
                                if nselected_table == nselect_table:
                                    nselect_field = nsplit_record[1]
                                    nselect_field_type = nsplit_record[2]
                                    print('   ' + str(nf_count) + '   |   ' + nselect_field + '  |  ' +
                                          nselect_field_type)
                                    nf_count += 1
                            print('')

                            # Prompt user go view different set of tables and columns
                            nview_another_table = ''
                            print('Would you like to check out another set of tables and columns before continuing?')
                            exit_return = user_input('   Enter Y/N: ')
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                return status
                            elif exit_return.split('|')[0] == 'continue':
                                nview_another_table = exit_return.split('|')[1]
                            if nview_another_table.lower() == 'y' or nview_another_table.lower() == 'yes':
                                pass
                            elif nview_another_table.lower() == 'n' or nview_another_table.lower() == 'no':
                                break

                # Prompt user to select tables for query builder
                r = 0
                sel_tables = ''
                print('Enter each of the tables to select by inputting either the table name or list number separated '
                      'by commas. You can join a maximum of three tables')
                print('   Examples of Format: table1,table2/1,2')
                exit_return = user_input('      Enter Table Names(s)/Table Number(s): ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    return status
                elif exit_return.split('|')[0] == 'continue':
                    sel_tables = exit_return.split('|')[1]
                s_sel_tables = sel_tables.replace(' ', '').split(',')
                selected_table_list = []
                for table in s_sel_tables:
                    if table not in table_list:
                        for k, v in table_dict.items():
                            if k == str(table):
                                selected_table = v
                                selected_table_list.append(v)
                    else:
                        selected_table = table
                        selected_table_list.append(table)

                # If multiple tables selected, prompt user to select field(s) to tie tables together by
                if ',' in sel_tables:

                    # Print list of fields from each table for user to select
                    sel_join_field_names = []
                    sel_unique_id_list = []
                    field_dict = {}
                    field_list = []
                    master_count = 1

                    # Iterate through selected table list and create inmemory dict for unique id fields for later
                    for user_select_table in selected_table_list:
                        print("\n" + user_select_table + "'s Fields  ")
                        join_field_list = []
                        for k, v in determine_primary_keys(user_select_table):
                            join_field_print_stmt = str(k).replace("('", "").replace("')", "").replace("', '", ", ") +\
                                  ' is the unique identifier'
                            print(join_field_print_stmt)
                            s_join_fields = join_field_print_stmt.replace(' ', '').split(',')
                            if len(s_join_fields) > 1:
                                for join_field in s_join_fields:
                                    join_field_list.append(join_field)
                            else:
                                join_field_list.append(join_field_print_stmt.replace(' ', ''))
                        dict_ent = {user_select_table: join_field_list}
                        primary_keys_dict.update(dict_ent)

                        # Print fields for user to choose from
                        print('Number  |     Field     |      Data Type')
                        f_count = 1
                        for record in in_mem_check_list:
                            split_record = record.split('|')
                            select_table = split_record[0]
                            if user_select_table == select_table:
                                select_field = split_record[1]
                                select_field_type = split_record[2]
                                f_dict_ent = {str(master_count) + '.' + str(f_count): str(user_select_table) + '.' +
                                                                                      select_field}
                                field_dict.update(f_dict_ent)
                                field_list.append(str(user_select_table) + '.' + select_field)
                                print(' ' + str(master_count) + '.' + str(f_count) + '   |   ' + select_field +
                                      '  |  ' + select_field_type)
                                f_count += 1
                        master_count += 1

                    # Prompt user for a field common to all three tables to join tables by
                    selected_table = selected_table_list
                    print('Enter field(s) that are present in each of your selected tables to act as a point of joining'
                          ' the tables.')
                    print('If you select multiple fields to join by please separate fields by commas.')
                    print('If the fields you want to select have different naming conventions, please use an equal.')
                    print('   Examples of Format: table.field,table.field2/1.1,1.2\n    '
                          'table.xfield=table2.yfield=table3.zfield')
                    exit_return = user_input('      Enter Field Name(s)/Field Number(s): ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        return status
                    elif exit_return.split('|')[0] == 'continue':
                        unique_id = exit_return.split('|')[1]
                        if ',' not in unique_id:
                            sel_unique_ids = unique_id
                            sel_unique_id_list.append(sel_unique_ids)
                        else:
                            sel_unique_ids = unique_id.split(',')
                            for s_unique_id in sel_unique_ids:
                                sel_unique_id_list.append(s_unique_id)

                    # Check if user entered field list number instead of unique field name
                    for unique_field in sel_unique_id_list:

                        # If selected fields to join by have different names
                        if '=' in unique_field:
                            alias_fields = unique_field.split('=')
                            len_alias_fields = len(alias_fields)
                            it_count = 0
                            unique_field_name = ''
                            for alias_field in alias_fields:
                                if alias_field not in field_list:
                                    for k, v in field_dict.items():
                                        if k == alias_field:
                                            if it_count < len_alias_fields - 1:
                                                unique_field_name += v + '='
                                                it_count += 1
                                            elif it_count == len_alias_fields - 1:
                                                unique_field_name += v
                                else:
                                    if it_count < len_alias_fields - 1:
                                        unique_field_name += alias_field + '='
                                        it_count += 1
                                    elif it_count == len_alias_fields - 1:
                                        unique_field_name += alias_field
                            sel_join_field_names.append(unique_field_name)

                        # If selected fields to join by have same names
                        else:
                            if unique_field not in field_list:
                                for k, v in field_dict.items():
                                    if k == unique_field:
                                        unique_field_name = v.split('.')[1]
                                        sel_join_field_names.append(unique_field_name)
                            else:
                                unique_field_name = sel_unique_ids.split('.')[1]
                                sel_join_field_names.append(unique_field_name)

                    # Print selected join fields to console for user
                    if len(sel_join_field_names) > 1:
                        enter_jf_count = 0
                        incr_str = ''
                        while enter_jf_count < len(sel_join_field_names):
                            if enter_jf_count < len(sel_join_field_names) - 1:
                                incr_str += sel_join_field_names[enter_jf_count] + ', '
                            else:
                                incr_str += 'and ' + sel_join_field_names[enter_jf_count]
                            enter_jf_count += 1
                        print(incr_str + ' selected as the fields to join the tables by!')
                        unique_field_name = incr_str.replace(', and ', ',').replace(', ', ',')
                        print(unique_field_name + '\n test')
                    else:
                        print(unique_field_name + ' selected as the field to join the tables by!')

                    # If user selects multiple tables to query from
                    sel_fields = ''
                    print('Enter the fields you want included in your query. Enter the name of the table with the'
                          ' field separated by a period \nYou can enter multiple fields separated by commas. \n'
                          'Do not re-enter the field(s) you are joining the tables by.')
                    print('   Examples of Format: table1.field1,table2.field2 or 1.1,2.2')
                    exit_return = user_input('      Enter Field Names(s)/Field Number(s): ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        return status
                    elif exit_return.split('|')[0] == 'continue':
                        sel_fields = exit_return.split('|')[1]
                    all_count = 0
                    final_field_list = []
                    final_field_dict = {}
                    s_sel_fields = sel_fields.replace(' ', '').split(',')

                # If user selects one table to query
                else:

                    # Print list of fields from each table for user to select
                    field_dict = {}
                    field_list = []
                    for user_select_table in selected_table_list:
                        for k, v in determine_primary_keys(user_select_table):
                            print(str(k).replace("('", "").replace("')", "").replace("', '", ", ") +
                                  ' is the unique identifier')
                        print("\n" + user_select_table + "'s Fields  ")
                        print('Number  |     Field     |      Data Type')
                        f_count = 1
                        for record in in_mem_check_list:
                            split_record = record.split('|')
                            select_table = split_record[0]
                            if user_select_table == select_table:
                                select_field = split_record[1]
                                select_field_type = split_record[2]
                                f_dict_ent = {str(f_count): select_field}
                                field_dict.update(f_dict_ent)
                                field_list.append(select_field)
                                print(str(f_count) + '   |   ' + select_field + '  |  ' + select_field_type)
                                f_count += 1

                    # If user only selects one table to query from
                    sel_fields = ''
                    print('Enter all to select all fields from table, or enter only the field name(s) or associated '
                          'field number(s) listed above that you want to select separated by commas')
                    print('   Examples of Format: fieldname1,fieldname2 or 1,2')
                    exit_return = user_input('      Enter Field Names(s)/Field Number(s)/All: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        return status
                    elif exit_return.split('|')[0] == 'continue':
                        sel_fields = exit_return.split('|')[1]
                    all_count = 0
                    final_field_list = []
                    final_field_dict = {}
                    s_sel_fields = sel_fields.replace(' ', '').split(',')

                # If user choices to run all
                if sel_fields.lower() == 'all' or sel_fields.lower() == 'a':
                    if ',' in sel_tables:
                        print('Selecting all fields from each selected table is not functional yet')
                        sys.exit(0)
                    else:
                        all_count += 1
                        final_field_list = field_list
                        for k, v in field_dict.items():
                            final_ent = {v: k}
                            final_field_dict.update(final_ent)

                # If user enters number instead of field name
                else:
                    for in_field in s_sel_fields:
                        if in_field not in field_list:
                            for k, v in field_dict.items():
                                if k == str(in_field):
                                    final_field_dict.update({v: k})
                                    final_field_list.append(v)
                        else:
                            final_field_dict.update({in_field: 'Empty'})
                            final_field_list.append(in_field)

                # Prompt user to enter a field(s) to filter by, if 'n', 'no', or blank string entered, then no filter
                while True:
                    print('Enter whether you want to add a filter to your query by entering the field(s) to filter'
                          ' the data by and giving the filters conditions. Enter N or No to not add a filter.')
                    print('   Examples of Format: filterfield1,filterfield2 or 1,2')
                    exit_return = user_input('      Enter Field Names(s)/Field Number(s)/No: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        return status
                    elif exit_return.split('|')[0] == 'continue':
                        filter_in = exit_return.split('|')[1]
                        if filter_in.lower() == 'no' or filter_in.lower() == 'n':
                            break
                        if ',' in filter_in:
                            s_filters_in = filter_in.split(',')
                            filter_list = []
                            for in_filter in s_filters_in:
                                if in_filter not in field_list:
                                    for k, v in field_dict.items():
                                        if k == str(in_filter):
                                            filter_list.append(v)
                                else:
                                    filter_list.append(filter_in)
                            sel_filter_in = str(set(filter_list)).replace("{'", "").replace("'}", "")\
                                .replace("', '", ",")

                        else:
                            if filter_in not in field_list:
                                for k, v in field_dict.items():
                                    if k == str(filter_in):
                                        sel_filter_in = v
                            else:
                                sel_filter_in = filter_in

                    # If user enters field(s) to filter by, prompt user for filter conditions for each field selected
                    filter_dict = {}
                    in_filter_cleaned = sel_filter_in.replace(' ', '')

                    # Check inputted fields to filter by for primary keys
                    for assoc_table, prim_filter in primary_keys_dict.items():
                        for filt_obj in sel_filter_in.split(','):
                            if '=' in filt_obj:
                                for filt_pair in filt_obj.split('='):
                                    print(prim_filter, filt_pair)
                                    if prim_filter.lower() == filt_obj.split('.')[1].lower():
                                        print(prim_filter + ' is a unique identifier and therefore is not an available'
                                                            ' filter option!')
                                        sel_filter_in -= assoc_table + '.' + prim_filter
                                        sel_filter_in -= ','
                            else:
                                if prim_filter.lower() == filt_obj.split('.')[1].lower():
                                    print(prim_filter + ' is a unique identifier and therefore is not an available'
                                                        ' filter option!')
                                    sel_filter_in -= assoc_table + '.' + prim_filter
                    print(sel_filter_in)

                    # If user selects multiple fields to filter by
                    forward_count = 0
                    if ',' in in_filter_cleaned:
                        for sel_filter in in_filter_cleaned.split(','):

                            # Prompt user to enter a condition for the selected filter
                            back_count = 0
                            filter_in = ''
                            print('Enter the condition you wish to give the filter field. You can use less than,'
                                  ' equal, greater than. \n'
                                  'You can also enter l or list to get a list of options to filter by in the data'
                                  ' or \n No to not use the selected field to filter and run the query')
                            print("   Examples of Format: equal 'some word', = 'some word', greater 20, greater"
                                  " equal 14, >= 11, etc")
                            exit_return = user_input('      Enter filter value for ' + sel_filter + ': ')
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                return status
                            elif exit_return.split('|')[0] == 'continue':
                                filter_in = exit_return.split('|')[1]

                            # If user opts to not use the filter.
                            if filter_in.lower() == 'no' or filter_in.lower() == 'n':
                                print(sel_filter + ' dropped from filters!')
                                back_count += 1

                            # If user chooses to see list of unique data enters with count to build filter from
                            if back_count == 0:
                                if filter_in.lower() == 'l' or filter_in.lower() == 'list':
                                    re_filter_option_list = get_filter_options(sel_filter, selected_table)
                                    print(sel_filter + "'s filter options")
                                    print('List Number  |  Filter Options')
                                    flt_count = 1
                                    for flt_opt in re_filter_option_list:
                                        print(str(flt_count) + '  |  ' + flt_opt)
                                        flt_count += 1

                                    # Prompt user to enter a condition for the selected filter
                                    filter_in = ''
                                    print('Enter the condition you wish to give the filter field. You can use'
                                          ' less than, equal, greater than.')
                                    print("   Examples of Format: equal 'some word', = 'some word', greater 20, "
                                          "greater equal 14, >= 11, etc")
                                    exit_return = user_input('      Enter filter value for ' + sel_filter + ': ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        return status
                                    elif exit_return.split('|')[0] == 'continue':
                                        filter_in = exit_return.split('|')[1]

                                    # If user opts to not use the filter.
                                    if filter_in.lower() == 'no' or filter_in.lower() == 'n':
                                        print(sel_filter + ' dropped from filters!')
                                        back_count += 1

                                if back_count == 0:
                                    determined_filter = determine_filter_value(filter_in)
                                    filt_dict_ent = {sel_filter: determined_filter}
                                    filter_dict.update(filt_dict_ent)
                                    forward_count += 1

                    # If user selects a single field to filter by
                    else:

                        # Prompt user to enter a condition for the selected filter
                        back_count = 0
                        filter_in = ''
                        print('Enter the condition you wish to give the filter field. You can use less than, equal,'
                              ' greater than. \n'
                              'You can also enter l or list to get a list of options to filter by in the data')
                        print("   Examples of Format: equal 'some word', = 'some word', greater 20, greater"
                              " equal 14, >= 11, etc")
                        exit_return = user_input('      Enter filter value for ' + in_filter_cleaned + ': ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            return status
                        elif exit_return.split('|')[0] == 'continue':
                            filter_in = exit_return.split('|')[1]
                        determined_filter = determine_filter_value(filter_in)
                        filt_dict_ent = {in_filter_cleaned: determined_filter}
                        filter_dict.update(filt_dict_ent)

                        # If user opts to not use the filter.
                        if filter_in.lower() == 'no' or filter_in.lower() == 'n':
                            print('Filter Dropped!')
                            back_count += 1

                        # If user chooses to see list of unique data enters with count to build filter from
                        if back_count == 0:
                            if filter_in.lower() == 'l' or filter_in.lower() == 'list':
                                re_filter_option_list = get_filter_options(in_filter_cleaned, selected_table)
                                print(in_filter_cleaned + "'s filter options")
                                print('List Number  |  Filter Options')
                                flt_count = 1
                                for flt_opt in re_filter_option_list:
                                    print(str(flt_count) + '  |  ' + flt_opt)
                                    flt_count += 1

                                # Prompt user to enter a condition for the selected filter
                                filter_in = ''
                                print('Enter the condition you wish to give the filter field. You can use less'
                                      ' than, equal, greater than. \n'
                                      'You can also enter l or list to get a list of options to filter by in the '
                                      'data')
                                print("   Examples of Format: equal 'some word', = 'some word', greater 20, "
                                      "greater equal 14, >= 11, etc")
                                exit_return = user_input('      Enter filter value for ' + in_filter_cleaned + ': ')
                                if exit_return.split('|')[0] == 'restart':
                                    status = 'restart'
                                    return status
                                elif exit_return.split('|')[0] == 'continue':
                                    filter_in = exit_return.split('|')[1]

                                # If user opts to not use the filter.
                                if filter_in.lower() == 'no' or filter_in.lower() == 'n':
                                    print('Filter Dropped!')
                                    back_count += 1

                            if back_count == 0:
                                determined_filter = determine_filter_value(filter_in)
                                filt_dict_ent = {in_filter_cleaned: determined_filter}
                                filter_dict.update(filt_dict_ent)
                                forward_count += 1

                    # Generate the filter string
                    if forward_count > 0:
                        filter_string_list = []
                        for k, v in filter_dict.items():
                            filter_str = k + ' ' + v
                            filter_string_list.append(filter_str)
                        filter_string = str(filter_string_list).replace("['", "").replace("']", "")\
                                .replace("', '", " AND ").replace('"]', '').replace('["', '').replace('", "', ' AND ')
                        break

                    else:

                        # Prompt user to determine if current filters are to be kept
                        diff_filt = ''
                        print('Would you like to select (a) different filter(s)?')
                        exit_return = user_input("   Enter Y/N: ")
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            return status
                        elif exit_return.split('|')[0] == 'continue':
                            diff_filt = exit_return.split('|')[1]

                        if diff_filt.lower() == 'n' or diff_filt.lower == 'no':
                            break

                # If yes, then prompt user to enter a limit on number of returned records
                limit_num = ''
                print('Enter how many records you want returned from your query or enter all to return all records')
                exit_return = user_input('   Enter Number of Rows/All: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    return status
                elif exit_return.split('|')[0] == 'continue':
                    limit_num = exit_return.split('|')[1]

            except:
                print('There was an error in your user input! \n Would you like to try again?')
                next_attempt = ''
                exit_return = user_input('    Enter Y/N: : ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    return status
                elif exit_return.split('|')[0] == 'continue':
                    next_attempt = exit_return.split('|')[1]
                if next_attempt.lower() == 'y' or next_attempt.lower() == 'yes':
                    r += 1
                elif next_attempt.lower() == 'n' or next_attempt.lower() == 'no':
                    x += 1
                else:
                    print('User input is not recognized!')

            '''
            # Optional error logging functionality
            except:
                try:
                    exc_info = sys.exc_info()

                finally:
                    # Display the *original* exception
                    traceback.print_exception(*exc_info)
                    del exc_info
            '''

            # Conditions to build different queries based on user input
            if x == 0 and r == 0:
                try:
                    if limit_num.lower() == 'all' or limit_num.lower() == 'a':
                        if all_count == 0:
                            re_data = sql_query('query', selected_table, final_field_list, sql_file_name=sql_file_name,
                                                add_editor=add_editor, overwrite=overwrite, filter_string=filter_string,
                                                unique_field_string=unique_field_name, join=' INNER JOIN ')
                        else:
                            re_data = sql_query('query', selected_table, sql_file_name=sql_file_name,
                                                add_editor=add_editor, overwrite=overwrite, filter_string=filter_string,
                                                unique_field_string=unique_field_name, join=' INNER JOIN ')
                    else:
                        if all_count == 0:
                            re_data = sql_query('query', selected_table, final_field_list, sql_file_name=sql_file_name,
                                                limit=int(limit_num), add_editor=add_editor, overwrite=overwrite,
                                                filter_string=filter_string, unique_field_string=unique_field_name,
                                                join=' INNER JOIN ')
                        else:
                            re_data = sql_query('query', selected_table, sql_file_name=sql_file_name,
                                                limit=int(limit_num), add_editor=add_editor, overwrite=overwrite,
                                                filter_string=filter_string, unique_field_string=unique_field_name,
                                                join=' INNER JOIN ')

                    # Prompt user to build another query
                    if re_data[0] != 'success':
                        sys.exit(0)
                    else:
                        print('Would you like to build another query?')
                        next_attempt = ''
                        exit_return = user_input('    Enter Y/N: : ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            return status
                        elif exit_return.split('|')[0] == 'continue':
                            next_attempt = exit_return.split('|')[1]
                        if next_attempt.lower() == 'y' or next_attempt.lower() == 'yes':
                            r += 1
                        elif next_attempt.lower() == 'n' or next_attempt.lower() == 'no':
                            x += 1
                        else:
                            print('User input is not recognized!')
                except:
                    print('\n Would you like to run another query?')
                    next_attempt = ''
                    exit_return = user_input('    Enter Y/N: : ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        return status
                    elif exit_return.split('|')[0] == 'continue':
                        next_attempt = exit_return.split('|')[1]
                    if next_attempt.lower() == 'y' or next_attempt.lower() == 'yes':
                        r += 1
                    elif next_attempt.lower() == 'n' or next_attempt.lower() == 'no':
                        x += 1
                    else:
                        print('User input is not recognized!')

    # If user wants to manually enter a SQL Statement
    elif query_assist.lower() == 'sql':
        in_sql = ''
        exit_return = user_input('Enter SQL Select Statement: ')
        if exit_return.split('|')[0] == 'restart':
            status = 'restart'
            return status
        elif exit_return.split('|')[0] == 'continue':
            in_sql = exit_return.split('|')[1]
        out_dir = cur_dir + '\\reports\\sql_queries\\'
        status = execute_sql_to_file(in_sql, out_dir, 'sql')
        if status == 'success' and add_editor and overwrite:
            save_sql_query(sql_query_dir, in_sql, sql_file_name=sql_file_name, overwrite=overwrite)
        if status == 'success' and add_editor and overwrite is False:
            save_sql_query(sql_query_dir, in_sql, sql_file_name=sql_file_name, overwrite=False)
        elif status == 'success' and add_editor is False:
            save_sql_query(sql_query_dir, in_sql)

    # If user enters an unrecognizable response
    else:
        print('Incorrect user input entered!')
        status = 'error'

    return status


# Function to list available saved SQL Queries
def list_sql_queries():
    status = 'success'
    sql_query_file = ''
    sql_dir = os.getcwd() + '\\sql_files\\queries\\'
    sql_c = 1
    sql_dict = {}
    sql_list = []

    # Iterate through .sql files in sql query directory and print ordered list to console
    print('file Number | SQL File')
    for root, dirs, files in os.walk(sql_dir):
        f_len = len(files)
        if f_len > 0:
            for f in files:
                print('     ' + str(sql_c) + '      | ' + f)
                sql_dict_ent = {str(sql_c): f}
                sql_dict.update(sql_dict_ent)
                sql_list.append(f)
                sql_c += 1
        else:
            print('No saved SQL query files were found')

    # Prompt user for report number or report name to run
    sel_sql_file = ''
    print('Enter the SQL Filename that you want to select, or the associated number listed from above')
    exit_return = user_input('   Enter filename/list number: ')
    if exit_return.split('|')[0] == 'restart':
        status = 'restart|none'
        return status
    elif exit_return.split('|')[0] == 'continue':
        sel_sql_file = exit_return.split('|')[1]

    # If user enters number instead of report name
    if sel_sql_file not in sql_list:
        for k, v in sql_dict.items():
            if k == str(sel_sql_file):
                sql_query_file = v
    else:
        sql_query_file = sel_sql_file

    return status + '|' + sql_query_file


# Function to list available reports
def list_reports():
    status = 'success'
    run_report = ''
    r_count = 1
    report_dict = {}
    report_list = []
    print('Report Number  |  Report  |  Output Type | Schedule Type | Day of Month | Day of Week | Run Time | SQL File')
    for r, t in get_report_dict():
        print(str(r_count) + '  |  ' + r.replace('-', ':') + ' | ' + t)
        report_ent = {str(r_count): r.split(' | ')[0]}
        report_dict.update(report_ent)
        report_list.append(r.split(' | ')[0])
        r_count += 1

    # Prompt user for report number or report name to run
    in_report2 = ''
    print('Enter the report name that you want to select, or the associated number listed above')
    exit_return = user_input('   Enter report/list number: ')
    if exit_return.split('|')[0] == 'restart':
        status = 'restart|none'
        return status
    elif exit_return.split('|')[0] == 'continue':
        in_report2 = exit_return.split('|')[1]

    # If user enters number instead of report name
    if in_report2.lower() not in report_list:
        for k, v in report_dict.items():
            if k == str(in_report2):
                run_report = v
    else:
        run_report = in_report2

    return status + '|' + run_report


# Function to modify the existing SQL reports
def modify_saved_sql(mode_type, sql_file, entered_sql='', add_editor=False, overwrite=True):
    sql_dir = os.getcwd() + '\\sql_files\\queries\\'
    status = 'success'

    # If user wants to delete saved SQL file
    if mode_type == 'delete':
        it_c = 0
        for dirs, root, files in os.walk(sql_dir):
            for f in files:
                if sql_file == f:
                    os.remove(sql_dir + f)
                    print(f + ' has been deleted from ' + sql_dir)
                    it_c += 1

        # If sql file to delete is not found in directory
        if it_c == 0:
            print(sql_file + ' was not found!')
            status = 'error'

    # If user wants to add new SQL file
    elif mode_type == 'add':

        # Create new SQL file
        new_sql_file = sql_dir + sql_file
        user_cursor = conn.cursor()

        # Prompt user to overwrite old sql file if it already exists
        if os.path.isfile(new_sql_file) and overwrite:

            # Prompt user to overwrite
            print(new_sql_file + ' already exist!. Do you want to overwrite the file?')
            over_write_re = ''
            exit_return = user_input('   Overwrite? Enter Y/N: ')
            if exit_return.split('|')[0] == 'restart':
                status = 'restart'
                return status
            elif exit_return.split('|')[0] == 'continue':
                over_write_re = exit_return.split('|')[1]

            # Read input to overwrite file or not
            if over_write_re.lower() == 'y' or over_write_re.lower() == 'yes':
                print(new_sql_file + ' will be overwritten!')
            elif over_write_re.lower() == 'n' or over_write_re.lower() == 'no':
                status = 'restart'
                return status

        # Test SQL Query
        new_sql = entered_sql
        if new_sql == '':
            while True:

                # Prompt user for SQL statement to file
                new_sql = ''
                print('Enter a SQL Select Statement to save, or enter list to list all available tables and '
                      'columns to choose from')
                exit_return = user_input('   Enter your SQL Select Statement/List: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    return status
                elif exit_return.split('|')[0] == 'continue':
                    new_sql = exit_return.split('|')[1]

                if new_sql.lower() == 'l' or new_sql.lower() == 'list':
                    status = list_db_tables_fields(add_editor, new_sql_file, overwrite)
                    return status

                # Test inputted query
                try:
                    user_cursor.execute(new_sql)
                    user_cursor.close()
                    print('Test of SQL Query was successful!')
                    break
                except:

                    # Prompt user to try another SQL query or not
                    print('SQL Query contains an error!')
                    try_again = ''
                    print('   Would you like to try another query?')
                    exit_return = user_input('      Enter Y/N: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                    elif exit_return.split('|')[0] == 'continue':
                        try_again = exit_return.split('|')[1]

                    # Read inputted user reply
                    if try_again.lower() == 'y' or try_again.lower() == 'yes':
                        pass
                    elif try_again.lower() == 'n' or try_again.lower() == 'no':
                        status = 'error'
                        user_cursor.close()
                        break
                    else:
                        print('Incorrect user input. Please enter either yes or no!')

        # Create new SQL file and write SQL Statement to it
        try:
            op_new_sql_file = open(new_sql_file, 'w')
            op_new_sql_file.write(new_sql)
            op_new_sql_file.close()
            print(new_sql_file + ' has been created!')
        except:
            print('SQL File save failed!')
            status = 'error'

    return status


# Create Database csv report
def run_saved_report(report_type):
    output_dir = os.getcwd() + '\\reports\\' + report_type + '\\'
    filename = report_type + '.csv'
    out_file = output_dir + filename

    # Run hard coded full database model report
    if report_type.lower() == 'database_model':
        headers = ['Table_Name', 'Field', 'Data_Type']
        data_list = []
        for t, ft in database_model_dict():
            for f, ty in ft.items():
                data_ent = [t, f, ty]
                data_list.append(data_ent)
        list_dict = build_data_dict(data_list, headers)
        write_to_csv_file(out_file, headers, list_dict)
        print(report_type + '.csv has been created and can be located at ' + output_dir + '!')

    # Run hard coded tables with data only report
    elif report_type.lower() == 'tables_with_data':
        headers = ['Table_Name', 'Table_Record_Count', 'Field', 'Data_Type']
        data_list = []
        for t, ftc in check_table_dict():
            c = ftc[0]
            ft = ftc[1]
            for f, ty in ft.items():
                data_ent = [t, str(c), f, ty]
                data_list.append(data_ent)
        list_dict = build_data_dict(data_list, headers)
        write_to_csv_file(out_file, headers, list_dict)
        print(report_type + '.csv has been created and can be located at ' + output_dir + '!')

    # Run SQL Based Reports
    else:
        sql_file_dir_dir = os.getcwd() + '\\sql_files\\queries\\'
        sql_file_name = ''
        o_type = ''
        for k, v in get_report_dict():
            if k.split(' | ')[0] == report_type:
                sql_file_name = v
                o_type = k.split(' | ')[1]
        in_sql = read_sql_file(sql_file_dir_dir, sql_file_name)
        execute_sql_to_file(in_sql, output_dir, 'report', report_type, o_type)

    return


# Function for user control of script via command line
def user_interface(loop_count):
    status = 'success'

    # Main user interface process infinite loop
    while True:

        # Intro printed during first iteration of the main while loop when program is kicked off
        if loop_count < 1:
            print('\nWelcome to the Timberline Data Tool Version 0.4!\n')
            print('System Engineered by Connor Sanders\nVersion Release Date 11/1/2016')
            print('You can enter e, q, exit, or quit at any point to exit the program. \n'
                  'You can also enter r, reset or restart at any point to restart from the beginning.')
            print('---------------------------------------------------------------------\n')
            print('Would you like to run or manage a SQL Query or Saved Report?')

        # Intro printed for each subsequent iteration
        else:
            print('\n---------------------------------------------------------------------\n')
            print('Would you like to run or manage another SQL Query or Saved Report?')

        # Main user input
        user_res = ''
        exit_return = user_input('   Enter SQL/Report: ')
        if exit_return.split('|')[0] == 'restart':
            status = 'restart'
            break
        elif exit_return.split('|')[0] == 'continue':
            user_res = exit_return.split('|')[1]

        # If user selects to input SQL command
        if user_res.lower() == 'sql':
            in_sql = ''
            query = 0
            list_count = 0
            cur_dir = os.getcwd()
            out_dir = cur_dir + '\\reports\\sql_queries\\'
            sql_file_dir = cur_dir + '\\sql_files\\queries\\'

            # User response two
            print('Do you want to work with your saved SQL Files, or manually enter a SQL Query to run?')
            user_res2 = ''
            exit_return = user_input('   Enter File/Query: ')
            if exit_return.split('|')[0] == 'restart':
                status = 'restart'
                break
            elif exit_return.split('|')[0] == 'continue':
                user_res2 = exit_return.split('|')[1]

            # If user wants to run SQL from file
            if user_res2.lower() == 'file':

                # Prompt user to run or edit saved SQL files
                run_build_file = ''
                print('Would you like to manage your saved SQL Files or run one?')
                exit_return = user_input('   Enter Run/Manage here: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    break
                elif exit_return.split('|')[0] == 'continue':
                    run_build_file = exit_return.split('|')[1]

                # If use wants to edit or create an existing query
                if run_build_file.lower() == 'manage':

                    # Prompt user to delete or add new saved query
                    del_add_sql_file = ''
                    print('Enter whether you want to create a new saved SQL File or delete an existing one')
                    exit_return = user_input('   Enter Add/Delete: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        del_add_sql_file = exit_return.split('|')[1]

                    # If user wants to create new SQL file
                    if del_add_sql_file.lower() == 'add':

                        # Prompt user for new report name
                        added_sql_file_name = ''
                        print('Enter the name of the new SQL file you want to add')
                        exit_return = user_input('   Enter Filename: ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            break
                        elif exit_return.split('|')[0] == 'continue':
                            added_sql_file_name = exit_return.split('|')[1]

                        # Get correct file name to add
                        checked_added_sql_name = determine_filename(sql_file_dir, added_sql_file_name, 'sql')
                        checked_file_name = checked_added_sql_name.split('\\')
                        len_checked_file = len(checked_file_name)
                        final_sql_file = checked_file_name[len_checked_file - 1]
                        status = modify_saved_sql('add', final_sql_file, add_editor=True)
                        break

                    # If user wants to delete SQL
                    elif del_add_sql_file.lower() == 'delete':

                        # Prompt user for sql file to delete
                        del_sql_file_name = ''
                        print('Enter the name of the SQL file you want to delete')
                        print('   You can also enter list for a list of all saved SQL files to choose from')
                        exit_return = user_input('      Enter SQL Filename/List: ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            break
                        elif exit_return.split('|')[0] == 'continue':
                            del_sql_file_name = exit_return.split('|')[1]

                        # If user wants printed saved sql file list
                        if del_sql_file_name.lower() == 'l' or del_sql_file_name.lower() == 'list':
                            re_status = list_sql_queries().split('|')
                            if re_status[0] == 'restart':
                                status = 'restart'
                                break
                            tar_sql_file = re_status[1]
                        else:
                            tar_sql_file = del_sql_file_name

                        # Get correct file name to delete
                        checked_added_sql_name = determine_filename(sql_file_dir, tar_sql_file, 'sql')
                        checked_file_name = checked_added_sql_name.split('\\')
                        len_checked_file = len(checked_file_name)
                        final_sql_file = checked_file_name[len_checked_file - 1]
                        status = modify_saved_sql('delete', final_sql_file)
                        break

                # If user wants to run a saved query
                elif run_build_file.lower() == 'run':

                    # SQL File User Response
                    sel_file = ''
                    print('Enter a SQL file to run here, or enter list to list available SQL Files to choose from')
                    exit_return = user_input('   Enter SQL Filename/List: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        sel_file = exit_return.split('|')[1]

                    # If user wants saved sql file list
                    if sel_file.lower() == 'l' or sel_file.lower() == 'list':
                        re_status = list_sql_queries().split('|')
                        if re_status[0] == 'restart':
                            status = 'restart'
                            break
                        saved_sql_file = re_status[1]
                    else:
                        saved_sql_file = sel_file

                    # Create SQL statement from user selected file
                    in_sql = read_sql_file(sql_file_dir, saved_sql_file)

            # If user wants to run a custom query
            elif user_res2.lower() == 'query':

                # SQL Command User Response
                in_sql = ''
                print('Enter a SQL statement to execute below, or enter list to list available tables and '
                      'columns to choose from')
                exit_return = user_input('   Enter SQL Statement/List: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    break
                elif exit_return.split('|')[0] == 'continue':
                    in_sql = exit_return.split('|')[1]
                query += 1

                # If user chooses to print a list of tables and fields
                if in_sql.lower() == 'l' or in_sql.lower() == 'list':
                    status = list_db_tables_fields(True)
                    list_count += 1
                    break

            # If incorrect user input
            else:
                print('Incorrect user input!')
                status = 'error'
                break

            # Execute SQL query
            if list_count == 0:
                re_status = execute_sql_to_file(in_sql, out_dir, 'sql')
                if re_status == 'error':
                    status = 'error'
                    break
                elif re_status == 'restart':
                    status = 'restart'
                    break

                # If running query sub-module instead of file
                if query > 0:

                    # SQL Command User Response
                    print('Would you like to save the entered query to a SQL file to run again later?')
                    save_query_re = ''
                    exit_return = user_input('   Enter Y/N: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        save_query_re = exit_return.split('|')[1]

                    # If user wants to save file
                    if save_query_re.lower() == 'y' or save_query_re.lower() == 'yes':
                        print('Enter the name for your saved SQL file')
                        sql_file = ''
                        exit_return = user_input('   Enter SQL filename: ')
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            break
                        elif exit_return.split('|')[0] == 'continue':
                            sql_file = exit_return.split('|')[1]

                        # Get correct file name to add
                        checked_added_sql_name = determine_filename(sql_file_dir, sql_file, 'sql')
                        checked_file_name = checked_added_sql_name.split('\\')
                        len_checked_file = len(checked_file_name)
                        final_sql_file = checked_file_name[len_checked_file - 1]
                        status = modify_saved_sql('add', final_sql_file, in_sql, add_editor=True)
                        break

                    # If user does not want to save file
                    elif save_query_re.lower() == 'n' or save_query_re.lower() == 'no':
                        break
            status = 'success'
            break

        # If user wants to run a report
        elif user_res.lower() == 'report':

            # Prompt user to choose either to run or build a report
            run_or_build_report = ''
            print('Do you want to run a report or build one?')
            exit_return = user_input('   Enter Run/Manage: ')
            if exit_return.split('|')[0] == 'restart':
                status = 'restart'
                break
            elif exit_return.split('|')[0] == 'continue':
                run_or_build_report = exit_return.split('|')[1]

            # If user selects to run report builder
            if run_or_build_report.lower() == 'manage':

                # Prompt user for to add a new report or delete an existing one
                print('Enter whether you want to add, edit, or delete a report')
                report_add_delete = ''
                exit_return = user_input('   Enter Add/Edit/Delete: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    break
                elif exit_return.split('|')[0] == 'continue':
                    report_add_delete = exit_return.split('|')[1]

                # If user wants to add a new report
                if report_add_delete.lower() == 'add':

                    # Prompt user for new report name
                    in_report_name = ''
                    print('Enter the name of the new report')
                    exit_return = user_input('   Enter New Report Name: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        in_report_name = exit_return.split('|')[1]

                    # check for and make output directory for new report
                    new_output_dir = os.getcwd() + '\\reports\\'
                    if not os.path.exists(new_output_dir + in_report_name):
                        os.makedirs(new_output_dir + in_report_name)
                        print(new_output_dir + in_report_name + ' has been created for report output files!')
                    else:
                        print('Report already exists!')
                        status = 'error'
                        break

                    # Prompt user for new report name
                    output_type = ''
                    print('Do you want your data in JSON or CSV Format?')
                    exit_return = user_input('   Enter JSON/CSV: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        output_type = exit_return.split('|')[1]

                    # Prompt user for SQL File to attach to report
                    sql_report_name = ''
                    print('Enter the name of the sql file you wish to build the report off of, '
                          'or enter list for a list of saved sql queries')
                    exit_return = user_input('   Enter SQL Filename/List: ',
                                             new_output_dir + in_report_name)
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        shutil.rmtree(new_output_dir + in_report_name + '\\')
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        sql_report_name = exit_return.split('|')[1]

                    # If user wants saved sql file list
                    if sql_report_name.lower() == 'l' or sql_report_name.lower() == 'list':
                        re_status = list_sql_queries().split('|')
                        if re_status[0] == 'restart':
                            status = 'restart'
                            break
                        saved_sql_file = re_status[1]
                    else:
                        saved_sql_file = sql_report_name

                    # Check to see if SQL file exists
                    sql_file_dir_dir = os.getcwd() + '\\sql_files\\queries\\'
                    sql_file_name = determine_filename(sql_file_dir_dir, saved_sql_file, 'sql')
                    s_sql_file_len = len(sql_file_name.split('\\'))
                    if not os.path.isfile(sql_file_name):
                        print(sql_file_name + ' does not exist. Please go into into the SQL Editor '
                                              'and save a .sql query.')
                        status = 'success'
                        break

                    # attempt to run report and export data into directory
                    sql_f_name = sql_file_name.split('\\')[s_sql_file_len - 1]
                    in_sql = read_sql_file(sql_file_dir_dir, sql_f_name)

                    # Attempt to execute user sql and return data to .csv or .json file
                    re_status = execute_sql_to_file(in_sql, new_output_dir + in_report_name + '\\', 'report',
                                                    in_report_name, output_type)
                    if re_status == 'error':
                        status = 'error'
                        shutil.rmtree(new_output_dir + in_report_name + '\\')
                        break
                    elif re_status == 'restart':
                        status = 'restart'
                        shutil.rmtree(new_output_dir + in_report_name + '\\')
                        break

                    # Prompt user to schedule report or not
                    schedule_answer = ''
                    print('Would you like to set this report up on a schedule?')
                    exit_return = user_input('   Y/N: ',
                                             new_output_dir + in_report_name)
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        shutil.rmtree(new_output_dir + in_report_name + '\\')
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        schedule_answer = exit_return.split('|')[1]

                    if schedule_answer.lower() == 'y' or schedule_answer.lower() == 'yes':

                        # Prompt user for daily, weekly, or monthly report schedule
                        schedule_freq = ''
                        print('Enter whether you want to schedule this report to run daily, weekly, or monthly.')
                        exit_return = user_input('   Daily/Weekly/Monthly: ',
                                                 new_output_dir + in_report_name)
                        if exit_return.split('|')[0] == 'restart':
                            status = 'restart'
                            shutil.rmtree(new_output_dir + in_report_name + '\\')
                            break
                        elif exit_return.split('|')[0] == 'continue':
                            schedule_freq = exit_return.split('|')[1]

                        # If user wants daily scheduled report
                        if schedule_freq.lower() == 'daily' or schedule_freq.lower() == 'd':

                            # Prompt user for daily, weekly, or monthly report schedule
                            daily_run_time = ''
                            run_time = ''
                            print('Enter the time of day you want this report run in HH:MM format.')
                            exit_return = user_input('   Enter daily runtime, or enter time for current server time: ',
                                                     new_output_dir + in_report_name)
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                shutil.rmtree(new_output_dir + in_report_name + '\\')
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                run_time = exit_return.split('|')[1]

                            # If user opts to see current time on server
                            if run_time.lower() == 'time' or run_time.lower() == 't':
                                print('The current server time is ' + get_time_str())

                                # Prompt user for daily, weekly, or monthly report schedule
                                print('Enter the time of day you want this report run in HH:MM format..')
                                exit_return = user_input('   Enter daily runtime: ',
                                                         new_output_dir + in_report_name)
                                if exit_return.split('|')[0] == 'restart':
                                    status = 'restart'
                                    shutil.rmtree(new_output_dir + in_report_name + '\\')
                                    break
                                elif exit_return.split('|')[0] == 'continue':
                                    run_time2 = exit_return.split('|')[1]
                                    daily_run_time = run_time2.replace(':', '-')
                            else:
                                daily_run_time = run_time.replace(':', '-')

                            modify_report_file('add', in_report_name, output_type, sql_f_name,
                                               schedule_type=schedule_freq.lower(), scheduled_run_time=daily_run_time)
                            break

                        # If user wants weekly scheduled report
                        elif schedule_freq.lower() == 'weekly' or schedule_freq.lower() == 'w':

                            # Prompt user for daily, weekly, or monthly report schedule
                            in_day_of_week = ''
                            print('Enter the day of the week you wish to schedule this report for')
                            exit_return = user_input('   Enter day of week: ',
                                                     new_output_dir + in_report_name)
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                shutil.rmtree(new_output_dir + in_report_name + '\\')
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                in_day_of_week = exit_return.split('|')[1]

                            # Function to get abbreviated day of week
                            day_of_week = determine_weekday(in_day_of_week)

                            # Prompt user for daily, weekly, or monthly report schedule
                            daily_run_time = ''
                            run_time = ''
                            print('Enter the time of day you want this report run in HH:MM format.')
                            exit_return = user_input('   Enter daily runtime, or enter time for current server time: ',
                                                    new_output_dir + in_report_name)
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                shutil.rmtree(new_output_dir + in_report_name + '\\')
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                run_time = exit_return.split('|')[1]

                            # If user opts to see current time on server
                            if run_time.lower() == 'time' or run_time.lower() == 't':
                                print('The current server time is ' + get_time_str())

                                # Prompt user for daily, weekly, or monthly report schedule
                                print('Enter the time of day you want this report run in HH:MM format..')
                                exit_return = user_input('   Enter daily runtime: ',
                                                        new_output_dir + in_report_name)
                                if exit_return.split('|')[0] == 'restart':
                                    status = 'restart'
                                    shutil.rmtree(new_output_dir + in_report_name + '\\')
                                    break
                                elif exit_return.split('|')[0] == 'continue':
                                    run_time2 = exit_return.split('|')[1]
                                    daily_run_time = run_time2.replace(':', '-')
                            else:
                                daily_run_time = run_time.replace(':', '-')

                            modify_report_file('add', in_report_name, output_type, sql_f_name,
                                               schedule_type=schedule_freq.lower(), day_of_week=day_of_week,
                                               scheduled_run_time=daily_run_time)
                            break

                        # If user wants monthly scheduled report
                        elif schedule_freq.lower() == 'monthly' or schedule_freq.lower() == 'm':

                            # Prompt user for daily, weekly, or monthly report schedule
                            in_day_of_month = ''
                            print('Enter the day of the month you wish to run this report on')
                            exit_return = user_input('   Enter day of month as an integer between 1 and 30: ',
                                                     new_output_dir + in_report_name)
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                shutil.rmtree(new_output_dir + in_report_name + '\\')
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                in_day_of_month = exit_return.split('|')[1]

                            # Prompt user for daily, weekly, or monthly report schedule
                            daily_run_time = ''
                            run_time = ''
                            print('Enter the time of day you want this report run in HH:MM format.')
                            exit_return = user_input('   Enter daily runtime, or enter time for current server time: ',
                                                     new_output_dir + in_report_name)
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                shutil.rmtree(new_output_dir + in_report_name + '\\')
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                run_time = exit_return.split('|')[1]

                            # If user opts to see current time on server
                            if run_time.lower() == 'time' or run_time.lower() == 't':
                                print('The current server time is ' + get_time_str())

                                # Prompt user for daily, weekly, or monthly report schedule
                                print('Enter the time of day you want this report run in HH:MM format..')
                                exit_return = user_input('   Enter daily runtime: ',
                                                         new_output_dir + in_report_name)
                                if exit_return.split('|')[0] == 'restart':
                                    status = 'restart'
                                    shutil.rmtree(new_output_dir + in_report_name + '\\')
                                    break
                                elif exit_return.split('|')[0] == 'continue':
                                    run_time2 = exit_return.split('|')[1]
                                    daily_run_time = run_time2.replace(':', '-')
                            else:
                                daily_run_time = run_time.replace(':', '-')

                            modify_report_file('add', in_report_name, output_type, sql_f_name,
                                               schedule_type=schedule_freq.lower(), day_of_month=in_day_of_month,
                                               scheduled_run_time=daily_run_time)
                            break

                    # If user does not want to set up code for
                    elif schedule_answer.lower() == 'n' or schedule_answer.lower() == 'no':
                        modify_report_file('add', in_report_name, output_type, sql_f_name)
                        status = 'success'
                        break

                    else:
                        print('user input is not recognized!')
                        status = 'error'
                        break

                elif report_add_delete.lower() == 'edit':

                    # Prompt user for report to edit
                    print('Enter which report you want to edit, or enter list for a list of all reports'
                          ' to choose from.')
                    ed_report = ''
                    exit_return = user_input('   Enter a Report/List: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        ed_report = exit_return.split('|')[1]

                    # If user wants report list
                    if ed_report.lower() == 'l' or ed_report.lower() == 'list':
                        re_status = list_reports().split('|')
                        if re_status[0] == 'restart':
                            status = 'restart'
                            break
                        edit_report = re_status[1]
                    else:
                        edit_report = ed_report

                    # Check to see if report exists and delete directory with files
                    new_output_dir = os.getcwd() + '\\reports\\'
                    if not os.path.exists(new_output_dir + edit_report + '\\'):
                        print('Report does not exists!')
                        status = 'error'
                        break
                    print(edit_report)

                    # Iterate through the report diction to select the selected reports metadata
                    for k, v in get_report_dict():
                        if k.split(' | ')[0] == edit_report:
                            sql_file = v
                            o_type = k.split(' | ')[1]
                            sched_type = k.split(' | ')[2]
                            dofm = k.split(' | ')[3]
                            dofw = k.split(' | ')[4]
                            run_time = k.split(' | ')[5]
                            read_metadata_dict = {'Report': edit_report, 'Output Type': o_type,
                                                  'SQL File': sql_file, 'Schedule': sched_type,
                                                  'Day of Week': dofw, 'Day of Month': dofm,
                                                  'Run Time': run_time}

                            meta_count = 1
                            m_field_dict = {}
                            m_field_list = []
                            print('List Number | Field | Setting')
                            for k2, v2 in read_metadata_dict.items():
                                if v2 != 'none' or k2 == 'Schedule' or k2 != 'Report':
                                    k_v_pair = k2 + ' | ' + v2.replace('-', ':')
                                    print(str(meta_count) + '   | ' + k_v_pair)
                                    dict_ent = {str(meta_count): k_v_pair}
                                    m_field_dict.update(dict_ent)
                                    m_field_list.append(k2)
                                    meta_count += 1

                            # Prompt user to select report metadata fields to change
                            print('Select (a) metadata field(s) to change. \nIf you enter multiple fields,'
                                  ' separate them by commas.')
                            fields_to_edit = ''
                            exit_return = user_input('   Enter a Field(s)/List Number(s): ')
                            if exit_return.split('|')[0] == 'restart':
                                status = 'restart'
                                break
                            elif exit_return.split('|')[0] == 'continue':
                                fields_to_edit = exit_return.split('|')[1]

                            # Check if user entered list number of field name
                            selected_field_list = []
                            s_fields_to_edit = fields_to_edit.replace(' ', '').split(',')
                            for m_field in s_fields_to_edit:
                                if m_field not in m_field_list:
                                    for k3, v3 in m_field_dict.items():
                                        if k3 == str(m_field):
                                            fields_to_edit = v3.split(' | ')[0]
                                            selected_field_list.append(fields_to_edit)
                                else:
                                    selected_field_list.append(m_field)

                            # Iterate through inputed metadata fields and prompt the user to give each a new setting
                            for edit_field in selected_field_list:
                                print('Enter a new value for ' + edit_field + ' to be set to...')
                                new_setting = ''
                                exit_return = user_input('   Enter a new setting: ')
                                if exit_return.split('|')[0] == 'restart':
                                    status = 'restart'
                                    break
                                elif exit_return.split('|')[0] == 'continue':
                                    new_setting = exit_return.split('|')[1]
                                if new_setting.lower() == 'n' or new_setting.lower() == 'no':
                                    pass
                                else:
                                    dict_ent = {edit_field: new_setting.replace(':', '-')}
                                    read_metadata_dict.update(dict_ent)

                            # Get function parameter variables from updated metadata dictionary
                            sql_file = read_metadata_dict['SQL File']
                            o_type = read_metadata_dict['Output Type']
                            new_sched_type = read_metadata_dict['Schedule']
                            dofm = read_metadata_dict['Day of Month']
                            dofw = read_metadata_dict['Day of Week']
                            run_time = read_metadata_dict['Run Time'].replace(':', '-')

                            # Check to see if the new schedule type is the same as the old schedule type
                            if new_sched_type.lower() != sched_type.lower():

                                # If report type metadata field is being changed to weekly
                                if new_sched_type.lower() == 'daily':
                                    print('Enter the time of day you want this report run in HH:MM format.')
                                    exit_return = user_input('   Enter daily runtime, or enter time for'
                                                             ' current server time: ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        break
                                    elif exit_return.split('|')[0] == 'continue':
                                        run_time = exit_return.split('|')[1]

                                    # If user opts to see current time on server
                                    if run_time.lower() == 'time' or run_time.lower() == 't':
                                        print('The current server time is ' + get_time_str())

                                        # Prompt user for daily, weekly, or monthly report schedule
                                        print('Enter the time of day you want this report run in HH:MM format..')
                                        exit_return = user_input('   Enter daily runtime: ')
                                        if exit_return.split('|')[0] == 'restart':
                                            status = 'restart'
                                            break
                                        elif exit_return.split('|')[0] == 'continue':
                                            run_time2 = exit_return.split('|')[1]
                                            run_time = run_time2.replace(':', '-')
                                    else:
                                        run_time = run_time.replace(':', '-')

                                # If report type metadata field is being changed to weekly
                                elif new_sched_type.lower() == 'weekly':
                                    in_day_of_week = ''
                                    print('Enter the day of the week you wish to schedule this report for')
                                    exit_return = user_input('   Enter day of week: ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        break
                                    elif exit_return.split('|')[0] == 'continue':
                                        in_day_of_week = exit_return.split('|')[1]

                                    # Function to get abbreviated day of week
                                    dofw = determine_weekday(in_day_of_week)

                                    # Prompt user for daily, weekly, or monthly report schedule
                                    run_time = ''
                                    print('Enter the time of day you want this report run in HH:MM format.')
                                    exit_return = user_input('   Enter daily runtime, or enter time for current '
                                                             'server time: ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        break
                                    elif exit_return.split('|')[0] == 'continue':
                                        run_time = exit_return.split('|')[1]

                                    # If user opts to see current time on server
                                    if run_time.lower() == 'time' or run_time.lower() == 't':
                                        print('The current server time is ' + get_time_str())

                                        # Prompt user for daily, weekly, or monthly report schedule
                                        print('Enter the time of day you want this report run in HH:MM format..')
                                        exit_return = user_input('   Enter daily runtime: ')
                                        if exit_return.split('|')[0] == 'restart':
                                            status = 'restart'
                                            break
                                        elif exit_return.split('|')[0] == 'continue':
                                            run_time2 = exit_return.split('|')[1]
                                            run_time = run_time2.replace(':', '-')
                                    else:
                                        run_time = run_time.replace(':', '-')

                                # If report type metadata field is being changed to monthly
                                elif new_sched_type.lower() == 'monthly':

                                    # Prompt user for day of month to run report on
                                    print('Enter the day of the month you wish to run this report on')
                                    exit_return = user_input('   Enter day of month as an integer between 1 and 30: ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        break
                                    elif exit_return.split('|')[0] == 'continue':
                                        dofm = exit_return.split('|')[1]

                                    # Prompt user for daily, weekly, or monthly report schedule
                                    run_time = ''
                                    print('Enter the time of day you want this report run in HH:MM format.')
                                    exit_return = user_input('   Enter daily runtime, or enter time for '
                                                             'current server time: ')
                                    if exit_return.split('|')[0] == 'restart':
                                        status = 'restart'
                                        break
                                    elif exit_return.split('|')[0] == 'continue':
                                        run_time = exit_return.split('|')[1]

                                    # If user opts to see current time on server
                                    if run_time.lower() == 'time' or run_time.lower() == 't':
                                        print('The current server time is ' + get_time_str())

                                        # Prompt user for daily, weekly, or monthly report schedule
                                        print('Enter the time of day you want this report run in HH:MM format..')
                                        exit_return = user_input('   Enter daily runtime: ')
                                        if exit_return.split('|')[0] == 'restart':
                                            status = 'restart'
                                            break
                                        elif exit_return.split('|')[0] == 'continue':
                                            run_time2 = exit_return.split('|')[1]
                                            run_time = run_time2.replace(':', '-')
                                    else:
                                        run_time = run_time.replace(':', '-')
                                elif new_sched_type == 'none':
                                    dofw = 'none'
                                    dofm = 'none'
                                    run_time = 'none'

                            # Execute modify report file function to edit metadata file
                            modify_report_file('edit', edit_report, o_type, sql_file,
                                               schedule_type=new_sched_type.lower(), day_of_month=dofm,
                                               day_of_week=dofw, scheduled_run_time=run_time)
                            status = 'success'
                            break

                # If user wants to delete an existing report
                elif report_add_delete.lower() == 'delete':

                    # Prompt user for report to delete
                    print('Enter which report you want to delete, or enter list for a list of all reports'
                          ' to choose from')
                    del_report = ''
                    exit_return = user_input('   Enter a Report/List: ')
                    if exit_return.split('|')[0] == 'restart':
                        status = 'restart'
                        break
                    elif exit_return.split('|')[0] == 'continue':
                        del_report = exit_return.split('|')[1]

                    # If user wants report list
                    if del_report.lower() == 'l' or del_report.lower() == 'list':
                        re_status = list_reports().split('|')
                        if re_status[0] == 'restart':
                            status = 'restart'
                            break
                        delete_report = re_status[1]
                    else:
                        delete_report = del_report

                    # Check to see if report exists and delete directory with files
                    new_output_dir = os.getcwd() + '\\reports\\'
                    if os.path.exists(new_output_dir + delete_report + '\\'):
                        shutil.rmtree(new_output_dir + delete_report + '\\')
                        print(new_output_dir + delete_report + ' has been deleted!')
                    else:
                        print('Report does not exists!')
                        status = 'error'
                        break

                    # Remove report from report list parameter file
                    sql_file = ''
                    o_type = ''
                    sched_type = 'none'
                    dofm = 'none'
                    dofw = 'none'
                    run_time = 'none'
                    for k, v in get_report_dict():
                        if k.split(' | ')[0] == delete_report:
                            sql_file = v
                            o_type = k.split(' | ')[1]
                            sched_type = k.split(' | ')[2]
                            dofm = k.split(' | ')[3]
                            dofw = k.split(' | ')[4]
                            run_time = k.split(' | ')[5]

                    modify_report_file('delete', delete_report, o_type, sql_file, schedule_type=sched_type,
                                       day_of_month=dofm, day_of_week=dofw, scheduled_run_time=run_time)
                    status = 'success'
                    break

            # If user chooses to run an existing report
            elif run_or_build_report == 'run':

                # Prompt user for a desired report to run
                in_report = ''
                print('Enter the report you want to run, or enter list for a list of report options to choose from')
                exit_return = user_input('   Enter a Report/List: ')
                if exit_return.split('|')[0] == 'restart':
                    status = 'restart'
                    break
                elif exit_return.split('|')[0] == 'continue':
                    in_report = exit_return.split('|')[1]

                # If user selects to look at list of reports
                if in_report.lower() == 'l' or in_report.lower() == 'list':
                    re_status = list_reports().split('|')
                    if re_status[0] == 'restart':
                        status = 'restart'
                        break
                    run_report = re_status[1]

                # If user enters report name
                else:
                    run_report = in_report
                try:
                    run_saved_report(run_report.lower())
                    break
                except:
                    print(run_report + ' failed to run!')
                    status = 'error'
                    break

        # If user enters an incorrect main option
        else:
            print('Incorrect user input entered!')
            status = 'error'
            break

    return status


# System main function
def main():

    # Iterate through user_interface function until the user decides to exit
    main_count = 0
    while True:
        status = user_interface(main_count)
        main_count += 1
        if status == 'success':
            exit_system_prompt()
        elif status == 'error':
            exit_system_prompt(True)

        elif status == 'restart':
            pass

    conn.close()

main()
