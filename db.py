import psycopg2
import configparser

# helpers
def list_to_string(l) -> str:
        str1 = " "
        return str1.join(l)

class Db:
    """ Connects to postgreSQL db """
    def __init__(self) -> None:
        self.db_params = dict()
        self.conn = None
        self.cur = None

    def config(self) -> None:
        config = configparser.ConfigParser()
        config.read("consum_project/config.ini")  # read config from config.ini file

        if config.has_section("postgresql"):  # ensure the postgresql section is in config.ini
            params = config.items("postgresql")
            for param in params:
                self.db_params[param[0]] = param[1]
        else:
            raise Exception("postgresql section missing in config.ini")
    
    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(**self.db_params)  # connect to db using the params read from config.ini
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def cursor(self) -> None:
        if self.conn:
            self.cur = self.conn.cursor()  # create cursor for executing SQL statements
        else:
            raise Exception("No connection established")
    
    def close(self) -> None:
        if self.conn is not None:
            self.conn.close
        else:
            raise Exception("No connection established")

    def create_schema(self) -> None:
        if self.cur:
            with open("consum_project/schema.sql") as reader:  # open schema.sql file
                schema = [line for line in reader]
            str_schema = list_to_string(schema)
            schema_split = str_schema.split(";")  # separate SQL statements
            for sch in schema_split[:-1]:
                statement = sch.replace("\n", "")
                statement += ";"
                self.cur.execute(statement)
                self.conn.commit()
    
    def prepare_conn(self) -> None:
        self.config()
        self.connect()
        self.cursor()
        self.create_schema()

db = Db()
err = psycopg2.DatabaseError