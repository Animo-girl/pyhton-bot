import sqlite3
import json
from datetime import datetime

tf = 'RC_2010-10'

sql_transaction = []

connection = sqlite3.connect('2010-11.db')

c = connection.cursor()

def create_table():
    c.execute("""create table if not exists parent_reply
  (parent_id text primary key , comment_id text unique, parent text,comment text,subreddit text, unix int,score int)""")
    print("table created")

def format_data(data):
    data = data.replace("\n"," newlinechar ").replace("\r"," newlinechar ").replace(' " '," ' ")
    return data


def find_existing_score(pid):
    try:
        sql = "select score from parent_reply where parent_id='{}'".format(pid)
        c.execute(sql)
        if result != None:
            return result[0]
        else :
            return false
    except Exception as e:
        return False


def find_parent(pid):
    try:
        sql = "select comment from parent_reply where comment_id='{}'".format(pid)
        c.execute(sql)
        if result != None:
            return result[0]
        else :
            return false
    except Exception as e:
        return False

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 10000:
        return False
    elif data=='[deleted]' or data=='[removed]':
        return False
    else:
        return True

def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('begin transaction ')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
            connection.commit()
            sql_transaction = []

def sql_insert_replace_comment(parentid,commentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('sREPLACE insertion', str(e))


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format( parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('sPARENT insertion', str(e))


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format( parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('sNO_PARENT insertion', str(e))



if __name__ == '__main__':
    create_table()
    paired_rows = 0
    row_counter = 0


    with open("RC_2010-10", buffering=10000) as f:
        for row in f:
            print(row)
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            comment_id = row['name']
            parent_data = find_parent(parent_id)

            if score >=2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(parent_id,comment_id,parent_data,body,subreddit,created_utc,score)
                            paired_rows+=1
                    else:
                        if parent_data:
                            sql_insert_has_parent(parent_id,comment_id,parent_data,body,subreddit,created_utc,score)
                        else:
                            sql_insert_no_parent(parent_id,comment_id,body,subreddit,created_utc,score)


            if row_counter % 100000 == 0:
                print("total rows:{},paired rows:{},time:{}".format(row_counter,paired_rows,str(datetime.now())))




 
