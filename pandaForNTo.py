import sqlite3
import pandas as pd

file = '2015-05'


for files in file:
    connection = sqlite3.connect("{}.db".format(file))
    c = connection.cursor()
    ##limit = 5000
    ##last = 0
   ## length = limi
    counter = 0
    length = 5000
    test = False
    while length == 5000:
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL AND score > 0 ORDER BY unix".format(last), connection)
        last = df.tail(1)['unix'].values[0]
        length = len(df)
        if not test:
            with open("parent_test.from","a",encoding="utf8") as f:
                for values in df['parent'].values:
                    f.write(content+"\n")
            with open("parent_test.from","a",encoding="utf8") as f:
                for values in df['comment'].values:
                    f.write(content+"\n")
            test = True
        else:
            with open("parent.to","a",encoding="utf8") as f:
                for values in df['parent'].values:
                    f.write(content+"\n")
            with open("comment.to","a",encoding="utf8") as f:
                for values in df['comment'].values:
                    f.write(content+"\n")
        counter+=1
        if counter % 20 == 0:
            print("Rows read: ",counter)




