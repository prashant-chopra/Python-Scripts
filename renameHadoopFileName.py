    from py4j.java_gateway import java_import
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(spark._jvm.Path('<temp_location>/part*'))[0].getPath().getName()
    fs.rename(spark._jvm.Path('<temp_location>' + file), spark._jvm.Path('<persistent_path>/<file_name>'+dateSuffix+'.orc'))
    fs.delete(spark._jvm.Path('<temp_location>'), True)

    '''
    More generic version below which accepts dataframe, tablename, zonename.
    Saving CSV to landing and ORC to Staging
    '''
    
    def writeFiles(fileDF,tableName,zoneName):

        landingDir = '/datalake/de/landing/'
        stagingDir = '/datalake/de/staging/'

        dropDir = landingDir if zoneName=='landing' else stagingDir
        fileType = 'csv' if zoneName=='landing' else 'orc'

        if fileType=='csv':
            tempDir = dropDir + tableName + "/" + dateSuffix + "/temp/"
            tableDir = dropDir + tableName + "/" + dateSuffix + "/"
        else:
            tempDir = dropDir + tableName + "/temp/"
            tableDir = dropDir + tableName + "/"

        fileDF.write.format(fileType).options(header="true").mode("append").save(tempDir)

        from py4j.java_gateway import java_import
        java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        file = fs.globStatus(spark._jvm.Path(tempDir + '/part*'))[0].getPath().getName()
        fs.rename(spark._jvm.Path(tempDir + file), spark._jvm.Path(tableDir + tableName + dateSuffix + "." + fileType))
        fs.delete(spark._jvm.Path(tempDir), True)
