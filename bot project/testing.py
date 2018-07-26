
def table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
     (parent_id TEXT PRIMARY KEY, commid TEXT UNIQUE,parent TEXT,
     comm TEXT, subr TEXT,unix INT,score INT)""")
    print("table")


def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data


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


def find_existing_score():
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


if __name__ == "_main_":
    table()
    print("table created")
    counter = 0
    paired_r = 0



    with open(r"C:/RC_{}".format(tf.split('-')[0],tf), buffering=1000) as f:
        for row in f:
            print("table created",row)
            counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            commid = row['commid']
            score = row['score']
            subr = row['subr']
            parent_data = find_parent(parent_id)
        if score >= 2:
            existing_comment_score = find_existing_score(parent_id)

