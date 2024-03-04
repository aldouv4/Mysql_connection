from sqlalchemy import create_engine
import mysql.connector as mysqlc


class Connection_mysql :
''' This class defines a way to connect to MySQL database'''
        
    def __init__ ( self , user , password  , host , database ) :    
    '''Give the necessary parameters to acces to database including :
    user name, password, url or ip for the database host and the database name'''

        #Create string for the engine
        
        db_connection_str ='mysql+pymysql://'+ user+ ':' + password + '@' + host + '/' + database 

        # Set the variable db_connection as an attribute 

        self.db_connection = create_engine(db_connection_str)

        



