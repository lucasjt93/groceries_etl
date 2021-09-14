import psycopg2
import configparser

# TODO: Finish connection

class Db:
    """ Connects to postgreSQL db """
    def __init__(self) -> None:
        self.db_params = dict()
        self.conn = None
        self.cur = None

    def config(self) -> None:
        config = configparser.ConfigParser()
        config.read("consum_project/config.ini")

        if config.has_section("postgresql"):
            params = config.items("postgresql")
            for param in params:
                self.db_params[param[0]] = param[1]
        else:
            raise Exception("postgresql section missing in config.ini")
    
    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print(f"Connected to db {self.conn}")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def cursor(self) -> None:
        if self.conn:
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT version()")
            db_version = self.cur.fetchone()
            print(db_version)


if __name__ == "__main__":
    db = Db()
    db.config()
    print(db.db_params)
    db.connect()
    db.cursor()