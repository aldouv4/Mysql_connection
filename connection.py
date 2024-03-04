from sqlalchemy import create_engine
import mysql.connector as mysqlc


class Connection_mysql :
        
    def __init__ ( self , user = 'root' , password = 'lapislazuli4' , host = 'localhost' , database = 'empresa' ) :

        db_connection_str ='mysql+pymysql://'+ user+ ':' + password + '@' + host + '/' + database 

        self.db_connection = create_engine(db_connection_str)

        # def dispose ( self ) : 



