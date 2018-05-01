def convertEpochToDateTime(eDate,fmt=None):

    sDate = int(eDate) / 1000000000.0
    dtDate = datetime.datetime.fromtimestamp(sDate).strftime('%Y-%m-%d %H:%M:%S.%f')

    if fmt=='hadooplex':
        dtDate = dtDate.replace('-', '.')
        dtDate = dtDate.replace(' ', 'T')

    return dtDate[:23]
