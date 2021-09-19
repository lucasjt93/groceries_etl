from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

import time
import csv
import os
import configparser
from pathlib import Path

import psycopg2

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

class Page:
    """ Extract: Scraps the consum web page to download tickets as PDF files to a local folder """
    def __init__(self) -> None:
        self.driver = None
        self.tickets = list()
        self.pdf_path = Path("consum_project/data/tickets_pdf")

    # get stored tickets from postgres
    def stored_db(self, db) -> None:
        db.prepare_conn()
        db.cur.execute("SELECT id FROM tickets;")
        self.tickets = [int(item[0]) for item in db.cur.fetchall()]
        db.close()
    
    # post tickets to postgres
    def ticket_to_db(self, db, id) -> None:
        statement = f"INSERT INTO tickets (id, created_at) VALUES ({id}, now());"
        db.prepare_conn()
        db.cur.execute(statement)
        db.conn.commit()
        db.close()

    # get webdriver for chrome
    def get_driver(self) -> None:
        # set webdriver options
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        prefs = {"download.default_directory": str(self.pdf_path)}
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(
            executable_path=r"/usr/bin/chromedriver",
            options=options
        )
        driver.implicitly_wait(10)
        driver.maximize_window()
        self.driver = driver

    # go to consum page
    def go_to_page(self) -> None:
        self.driver.get("https://mundoconsum.consum.es/auth/index")
        assert "Consum" in self.driver.title

        # accept cookies
        button = self.driver.find_element_by_id("onetrust-accept-btn-handler")
        ActionChains(self.driver).move_to_element(button).click(button).perform()

    # login using credentials from config
    def login(self) -> None:
        config = configparser.ConfigParser()
        config.read("consum_project/config.ini")
        username = self.driver.find_element_by_id("login")
        username.clear()
        username.send_keys(config["credentials"].get("user"))
        password = self.driver.find_element_by_id("password")
        password.clear()
        password.send_keys(config["credentials"].get("pass"))

        button = self.driver.find_element_by_class_name("btn_generico_orange")
        ActionChains(self.driver).move_to_element(button).click(button).perform()

    # rename pdf after download to ticket id
    def rename_file(self, ticket_id) -> None:
        time.sleep(1)  # wait for ticket to download
        while True:
            try:
                old_file = os.path.join(self.pdf_path, "ticket.pdf")
                new_file = os.path.join(self.pdf_path, f"{ticket_id}.pdf")
                time.sleep(1)
                os.rename(old_file, new_file)
                break
            except FileNotFoundError:  # in case the ticket hasn't been downloaded yet
                continue

    # perform actions to download tickets
    def download_ticket(self, ticket_id) -> None:
        menu = self.driver.find_element_by_id("menu-puntos")
        ActionChains(self.driver).move_to_element(menu).click(menu).perform()
        download = self.driver.find_element_by_id("dropdown1")
        ActionChains(self.driver).move_to_element(download).click(download).perform()
        print(f"ticket {ticket_id} downloaded")
        self.rename_file(ticket_id)
        self.driver.back()  # go back to tickets list

    def get_tickets(self) -> None:
        self.driver.get("https://mundoconsum.consum.es/es/personal/tickets")  # instantiate tickets object
        self.driver.get("https://nomastickets.consum.es/app/mytickets.html")  # move to tickets frame
        assert "Tickets" in self.driver.title

        # drag and drop to load all elements until end. I need to check all displayed tickets everytime as the list
        # goes back to top after downloading a ticket :(
        while self.driver.find_element_by_class_name("pullUpLabel").text != "Tickets de los últimos 90 días.":
            displayed_tickets = [
                displayed.get_attribute("id")
                for displayed
                in self.driver.find_elements_by_class_name("panel-default")
            ]

            for ticket_id in displayed_tickets:
                if int(ticket_id) not in self.tickets:  # check that ticket is not yet scrapped
                    self.tickets.append(int(ticket_id))
                    self.ticket_to_db(db, ticket_id)
                    
                    while True:
                        try:
                            ticket = self.driver.find_element_by_id(ticket_id)  # retrieve ticket element with id
                            break
                        except NoSuchElementException:  # this is for the reloading, i need to scroll down again
                            ActionChains(self.driver).move_to_element(
                                self.driver.find_element_by_class_name("pullUpLabel")).perform()
                            ActionChains(self.driver).drag_and_drop(self.driver.find_element_by_class_name("pullUpLabel"),
                                                                    self.driver.find_element_by_class_name(
                                                                        "l10n-tickets")).perform()

                    ActionChains(self.driver).move_to_element(ticket).click(ticket).perform()  # enter ticket
                    try:
                        self.download_ticket(ticket_id)
                    except NoSuchElementException:
                        ActionChains(self.driver).move_to_element(ticket).click(ticket).perform()  # make sure it enters
                        self.download_ticket(ticket_id)

            # scroll down
            ActionChains(self.driver).move_to_element(self.driver.find_element_by_class_name("pullUpLabel")).perform()
            ActionChains(self.driver).drag_and_drop(self.driver.find_element_by_class_name("pullUpLabel"),
                                                    self.driver.find_element_by_class_name("l10n-tickets")).perform()
            print("Checking, relax :)")

    # close driver
    def teardown(self) -> None:
        self.driver.close()

    def scrap(self) -> None:
        # lets take the time
        start_time = time.time()
        print(f"Started at {time.strftime('%Y-%m-%d %H:%M:%SZ', time.localtime(start_time))}")

        # get the driver ready + configs
        self.get_driver()
        self.stored_db(db)

        # scrapes the page
        self.go_to_page()
        self.login()
        self.get_tickets()

        # close the driver
        self.teardown()

        # stop timer
        end_time = time.time()

        print(f"Finalized at {time.strftime('%Y-%m-%d %H:%M:%SZ', time.localtime(end_time))}")
        print(f"Total time: {end_time - start_time}")


class TicketParser:
    """ Transform: Parse the tickets.txt to .sql file to upload to postgresql """
    # TODO: Finish parser after finishing db

    def __init__(self) -> None:
        self.tickets_path = Path("consum_project/data/tickets_pdf")
        self.tickets_txt = None

    # load tickets.txt into class attributes
    def load_txt(self) -> None:
        txt_files = os.listdir(self.tickets_path)
        self.tickets_txt = [txt for txt in txt_files if txt[-4:] == ".txt"]  # Only .txt files

    # parse products data from tickets
    def get_products(self, ticket_parsed) -> dict(list()):
        parsed = dict()
        quantity = list()
        product = list()
        pvp = list()
        total = list()
       
        for n in range(7, len(ticket_parsed)):  # parsed[7] is where the products start in the ticket
            line = ticket_parsed[n]  # current line in ticket

            if any(code in line for code in ("2902614104014", "2911866831005")):   # this is where the product list ends
                break
            
            quantity.append(line[0:5])
            product.append(line[5:28])
            pvp.append(line[28:32])
            total.append(line[32:38])
        
        parsed["quantity"] = quantity
        parsed["product"] = product
        parsed["pvp"] = pvp
        parsed["total"] = total

        return parsed

    # TODO load to db
    def post_to_db(self, parsed_products, db, ticket_id):
        statement = f"INSERT INTO products (ticket_id, quantity, product, pvp, total) VALUES "
        print(len(parsed_products["product"]))
        for n in range(len(parsed_products["product"])):
            for i in parsed_products.keys():
                print(i, parsed_products[i][n].strip())
    
            #db.prepare_conn()
            #db.cur.execute(statement)
            #db.conn.commit()
            #db.close()


    def read_txt(self) -> None:
        for txt in self.tickets_txt:
            print("ticket", txt)
            path_to_txt = os.path.join(self.tickets_path, f"{txt}")
            with open(f"{path_to_txt}") as ticket:
                t = [line for line in ticket]
            parsed = self.get_products(t)
            self.post_to_db(parsed, db, txt)




# instantiate object for calling from DAG
def scrapper() -> None:
    consum = Page()
    consum.scrap()


if __name__ == '__main__':
    # Scrape consum page to retrieve tickets
    #consum = Page()
    #consum.scrap()

    parser = TicketParser()
    parser.load_txt()
    parser.read_txt()