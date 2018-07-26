import sqlite3
import json
from datetime import datetime

tf = '2010-10'
sql_trans = []

conn = sqlite3.connect('{}.db'.format(tf))
c = conn.cursor()


def table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY , commid TEXT UNIQUE, parent TEXT, sr TEXT, unix INT,score INT, comment TEXT, subreddit TEXT  )")

def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

def find_score(pid):
    try:

        sql = "SELECT comment FROM parent_reply WHERE parent_id='{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
         return False

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) <1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def find_parent(pid):
    try:

        sql = "SELECT comm FROM parent_reply WHERE commid='{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

def transacbld(sql):
    global sql_trans
    sql_trans.append(sql)
    if len(sql_trans) > 1000:
        c.execute('BEGIN TRANSACTION ')
        for s in sql_trans:
            try:
                c.execute(s)
            except:
                pass
        conn.commit()
        sql_trans =[0]


def insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """update parent_reply set parent_id = ?, commid = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? where parent_id = ?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transacbld(sql)
    except Exception as e:
        print(str(e))

def insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """insert into parent_reply(parent_id, commid,parent,commnet,subreddit,unix,score) values ("{}","{}","{}","{}","{}","{}","{},"{}")""".format(parentid, commid, parent, comment,subreddit,int(time),score)
        transacbld(sql)
    except Exception as e:
        print(str(e))


def insert_no_parent(commentid,parentid,parent, comment,subreddit,time,score):
    try:
        sql = """insert into parent_reply(parent_id, commid,parent,commnet,subreddit,unix,score) values ("{}","{}","{}","{}","{}","{}","{}")""".format(parentid,commentid,comment,subreddit,int(time),score)
        transacbld(sql)
    except Exception as e:
        print(str(e))



if __name__ == "__main__":
    table()
    rowc = 0
    pr = 0

    with open("RC_2010-10", buffering = 1000) as f:
        for row in f:
            rowc += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body =format_data(row['body'])
            created_utc = row['created_utc']
            commid = row['name']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score >= 2:
                if acceptable(body):
                    existing_score = find_score(parent_id)
                    if existing_score:
                        if score > existing_score:
                            insert_relace_comment(commid,parent_id,parent_data,body,subreddit,created_utc,score)
                    else:
                        if parent_data:
                            insert_has_parent(commid,parent_id,parent_data,body,subreddit,created_utc,score)
                            rowc+=1
                        else:
                            insert_no_parent(commid,parent_id,parent_data,body,subreddit,created_utc,score)

            if rowc % 100000 == 0:
                print("totaal rows:{}, paired rows:{},time:{}".format(rowc,pr,str(datetime.now())))



 