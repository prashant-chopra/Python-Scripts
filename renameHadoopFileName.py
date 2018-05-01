    from py4j.java_gateway import java_import
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(spark._jvm.Path('<temp_location>/part*'))[0].getPath().getName()
    fs.rename(spark._jvm.Path('<temp_location>' + file), spark._jvm.Path('<persistent_path>/<file_name>'+dateSuffix+'.orc'))
    fs.delete(spark._jvm.Path('<temp_location>'), True)
