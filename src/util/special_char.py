# speclial char validation and find / replace
from pyspark.sql.functions import *





class CharValidation:

    def specialcharvalidation(x):


            a=''

            for i in x:

                if i not in codes:

                    a = a + i


                else:
                    return ""

            return a


    def wordfindreplace(x,find,replace):

           if x in find:
               return replace

           else:
               return x

    def wordremove(x,find,null):

           if x in find:
               return null

           else:
               return x

    def charfindreplace(x, find, replace):

        a=''
        for i in x:

           if i in find:

                a=a+replace

                continue

           else:
               a=a+i


        return a

    def specialcharreplace(x,replace):

        a = ''
        for i in x:

            if i in codes:

                a = a + replace

                continue

            else:
                a = a + i

        return a

    def specialcharremove_1spc_null(x):
        a= len(x)
        if a >1 :


            a=''

            for i in x:

                if i in codes:


                    continue


                else:
                    a=a+i


            return a

        else:
            asc

    def specialcharremove(x):

            a=''

            for i in x:

                if i in codes:


                    continue


                else:
                    a=a+i


            return a

    def date(x):


        a=len(x)

        if a<10:
            z = '0' + x[0:]
            return z

        else:
            return x











codes = ['(', '%','$', '&', '*','~','@','^',')','-','/','#','\\']





