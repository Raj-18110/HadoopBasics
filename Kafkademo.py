from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    clusterPath = "/opt/spark/work-dir/job-1"
    localPath = "output"
    data = [
        ["1201", "satish", "25"],
        ["1202", "krishna", "28"],
        ["1203", "smith", "39"],
        ["1204", "jaded", "23"],
        ["1205", "prude", "23"]
    ]
    dataColumns = ["id", "name", "age"]
    sparkSession = SparkSession.builder.appName("pyspark").getOrCreate()

    deptDF = sparkSession.createDataFrame(data=data, schema=dataColumns)
    deptDF.printSchema()
    deptDF.coalesce(1).write.option("header", True).csv(localPath)
