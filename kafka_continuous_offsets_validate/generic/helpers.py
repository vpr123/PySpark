def say(txt):
    print("==> " + txt)

def createView (df, view_name):
    print("\n-- " + view_name + "\n")
    df.createOrReplaceTempView(view_name)
    df.printSchema()
    df.show()

def getSQL(file_path):
    import os;
    say("Current workdir is: " + os.getcwd())
    file = open(file_path, "r")
    content = file.read()
    file.close()
    return content

def runFinalSQL (spark, file_path):
    s = getSQL(file_path)
    print("\n" + s)
    res = spark.sql(s)
    res.printSchema()
    return res

def runSQL (spark, file_path, view_name, params=None):
    s = getSQL(file_path)

    if params is not None:
        for k, v in params.items():
           s = s.replace("$$" + k + "$$", v);

    print("\n-- " + view_name + "\n" + s)
    res = spark.sql(s)
    res.cache()
    res.createOrReplaceTempView(view_name)
    res.printSchema()

def runQuery(spark, query, num_rows=None):
    say(query)
    res = spark.sql(query)
    if num_rows is None:
       res.show(100, False)
    else:
       res.show(num_rows, False)

