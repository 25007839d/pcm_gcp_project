from datetime import datetime



def logfun(a):

    # datetime object containing current date and time
    current_time = datetime.now()
    current_time1 = current_time.strftime("%d-%m-%Y %H.%M.%S")

    newpath =a+current_time1
    return newpath

#
a=logfun(r'C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\log')
print(a)