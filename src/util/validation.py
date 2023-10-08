from pyspark.sql.functions import *

def spec_char_val(df,col_nam):
    '''
    before validation we changed null value in null string for itration
    :param df: pass data frame
    :param col_nam: pass column for validation
    :return: validated data frame
    '''

    if col_nam!=['']:
        for i in col_nam:

            def specialcharvalidation(x):
                a=''

                for i in x:

                    if i not in codes:

                        a = a + i


                    else:
                        return ""

                return a
            codes = ['(', '%','$', '&', '*','~','@','^',')','/','#','\\']


            spchar = udf(lambda z: specialcharvalidation(z), StringType())
            df=df.withColumn(i,regexp_replace(col(i),'null','null')).withColumn(i, spchar(col(i))).where(col(i) != '')

        return df
    else:
     return df

def mob_num_val(df,col_nam):
   '''

    :param df: pass dataframe
    :param col_nam: pass column name for validation
    :return: validated dataframe
   '''
   if col_nam!=['']:
           for i in col_nam:

            def null(z): # check mob no len and 0 to 9 digit

                if len(z)==10 :
                   for i in z:
                    if i in ['0','1','2','3','4','5','6','7','8','9']:
                     return z

                else:
                    return ""



            spchar = udf(lambda z: null(z), StringType()) # udf function
            df = df.withColumn(i,regexp_replace(col(i),'null','null')).withColumn(i, spchar(col(i))).where(col(i) != '')

            return df
   else:
       return df



def email_val(df, col_nam):
    '''

    :param df: pass dataframe
    :param col_nam: pass column name for validatin
    :return: validated dataframe
    '''
    if col_nam!=['']:
          for i in col_nam:

              df=df.filter(col(i).rlike(r'^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'))
              return df
    else:
        return df


def null_val(df, col_nam):
    '''
    filter the not null
    :param df: pass dataframe
    :param col_nam: pass column name for validatin
    :return: validated dataframe
    '''
    if col_nam != ['']:
        for i in col_nam:
            df = df.filter(col(i)!= 'null')
            return df
    else:
        return df

