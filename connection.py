from sqlalchemy import create_engine
import mysql.connector as mysqlc
import pandas as pd


class Connection_mysql :
    ''' This class defines a way to connect to MySQL database using sqlalchemy'''
#------------------------------------------------------------------------------------------------------------------------------------------------       
   
    def __init__ ( self , user , password  , host , database ) :
        '''Give the necessary parameters to acces to database including :
        user name, password, url or ip for the database host and the database name'''

        #Create string for the engine        
        db_connection_str ='mysql+pymysql://'+ user+ ':' + password + '@' + host + '/' + database 

        # Set the variable db_connection as an attribute 
        self.db_connection = create_engine(db_connection_str)
#------------------------------------------------------------------------------------------------------------------------------------------------

    def read_data ( self , query_extract_data ) :
        '''Creates a pandas DataFrame from MySQL database table. Example
        table_name = table_name
        query_extract_data = f'SELECT * FROM {table_name}' '''

        #Use pandas to extract the selected table's information to create a DataFrame
        df = pd.sql_read ( query_extract_data , self.db_connection )

        return df
#------------------------------------------------------------------------------------------------------------------------------------------------

    def delete_data (self , selected_table , conditional_column , condition ) :
        ''' Delete rows depending on selected condition
        selected_table = str, database table to delet rows
        conditional_column = str , column containing the conditional statement 
        condition = str, conditional value or values selected to drop from the table '''

        #Set the query statement for rows deletion
        query_delete = '''DELETE FROM ''' + selected_table + ''' WHERE ''' + conditional_column + ''' IN ''' + f'(condition)'

        #Execute query
        self.db_connection.execute ( query_delete )
#------------------------------------------------------------------------------------------------------------------------------------------------
           
    def upload_data ( self , df , selected_table ) :
        '''Upload multiple rows from a pandas DataFrame to selected database table from MySQL
        df= pd DataFrame, contains the rows to upload
        selected_table = str, MySQL table name to upload values '''

        #Use pandas method 'to_sql' to load rows to the selected table
        df.to_sql ( 
            con = self.db_connection,  #Define the connection
            name = selected_table ,    #Defines selected table
            if_exists = 'append',      #Declares append if it exists, otherwise the table is created
            index = False,             #Avoids upload df index to the table
            chunksize=500,             #Batch uploading, if number of rows is too big it avoids pc burnout
            method ='multi'            #Defines multiple rows are appended
        )    
#------------------------------------------------------------------------------------------------------------------------------------------------
    
    def create_table (self , table_name , columns_names ):
        '''Creates a new table for the database
        table_name : str, name of the new table
        columns_names = str, names and type of each column from the table'''

        #Create table with the statement 'if not exists'    
        db_connection.execute ( '''CREATE TABLE if not exists ''' + table_name +  f'(columns_names)' )
#------------------------------------------------------------------------------------------------------------------------------------------------
        
    def delete_Table ( self, table_name ): 
        '''Deletes table from database
        table_name = str, table to be dropped'''

        #Execute query to drop the selected table, add statement 'if exists' 
        db_connection.execute ( '''DROP TABLE IF EXISTS ''' + table_name  )
 #------------------------------------------------------------------------------------------------------------------------------------------------
 
    def dispose ( self ) :     
        ''' Dispose connection''' 
   
        self.db_connection.dispose ( )