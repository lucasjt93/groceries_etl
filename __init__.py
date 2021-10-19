from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

import time
import os
import configparser
from pathlib import Path

from consum_project.db import db, err
from psycopg2 import sql

# path to folder where tickets are stored
pdf_path = Path("consum/consum_project/data/tickets_pdf")


class Page:
    """ Extract: Scraps the consum web page to download tickets as PDF files to a local folder """

    def __init__(self) -> None:
        self.driver = self.get_driver(pdf_path)
        self.tickets = self.stored_db()

    # get stored tickets from postgres
    def stored_db(self) -> list():
        db.prepare_conn()
        db.cur.execute(sql.SQL("SELECT {field} FROM {table}").format(field=sql.Identifier("id"), table=sql.Identifier("tickets")))
        db.close()
        return [int(item[0]) for item in db.cur.fetchall()]
    
    # post tickets to postgres
    def ticket_to_db(self, id) -> None:
        db.prepare_conn()
        db.cur.execute(sql.SQL("INSERT INTO {} values (%s, %s)").format(sql.Identifier("tickets")), [id, "now()"])
        db.conn.commit()
        db.close()

    # get webdriver for chrome
    def get_driver(self, pdf) -> webdriver:
        # set webdriver options
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        prefs = {"download.default_directory": str(pdf)}
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(
            executable_path=r"/usr/bin/chromedriver",
            options=options
        )
        driver.implicitly_wait(10)
        driver.maximize_window()
        return driver

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
        config.read("consum/consum_project/config.ini")
        username = self.driver.find_element_by_id("login")
        username.clear()
        username.send_keys(config["credentials"].get("user"))
        password = self.driver.find_element_by_id("password")
        password.clear()
        password.send_keys(config["credentials"].get("pass"))

        button = self.driver.find_element_by_class_name("btn_generico_orange")
        ActionChains(self.driver).move_to_element(button).click(button).perform()
    
    def scroll_down(self) -> None:
        ActionChains(self.driver).move_to_element(self.driver.find_element_by_class_name("pullUpLabel")).perform()
        ActionChains(self.driver).drag_and_drop(self.driver.find_element_by_class_name("pullUpLabel"),self.driver.find_element_by_class_name("l10n-tickets")).perform()

    # rename pdf after download to ticket id
    def rename_file(self, ticket_id) -> None:
        time.sleep(1)  # wait for ticket to download
        while True:
            try:
                old_file = os.path.join(pdf_path, "ticket.pdf")
                new_file = os.path.join(pdf_path, f"{ticket_id}.pdf")
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
                    self.ticket_to_db(ticket_id)
                    
                    while True:
                        try:
                            ticket = self.driver.find_element_by_id(ticket_id)  # retrieve ticket element with id
                            break
                        except NoSuchElementException:  # this is for the reloading, i need to scroll down again
                            self.scroll_down()

                    ActionChains(self.driver).move_to_element(ticket).click(ticket).perform()  # enter ticket
                    try:
                        self.download_ticket(ticket_id)
                    except NoSuchElementException:
                        ActionChains(self.driver).move_to_element(ticket).click(ticket).perform()  # make sure it enters
                        self.download_ticket(ticket_id)

            self.scroll_down()
            print("Checking, relax :)")

    def scrap(self) -> None:
        # lets take the time
        start_time = time.time()
        print(f"Started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

        # scrapes the page
        self.go_to_page()
        self.login()
        self.get_tickets()

        # close the driver
        self.driver.close()

        # stop timer
        end_time = time.time()

        print(f"Finalized at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
        print(f"Total time: {end_time - start_time}")


class TicketParser:
    """ Transform: Parse the tickets.txt to .sql file to upload to postgres """

    def __init__(self) -> None:
        self.products_loaded = self.loaded()
        self.tickets_txt = self.load_txt()
        self.errors = list()

    def loaded(self) -> list():
        db.prepare_conn()
        db.cur.execute(sql.SQL("SELECT DISTINCT({field}) FROM {table}").format(field=sql.Identifier("ticket_id"), table=sql.Identifier("products")))
        db.close()
        return [int(item[0]) for item in db.cur.fetchall()]

    # load tickets.txt into class attributes
    def load_txt(self) -> list():
        txt_files = os.listdir(pdf_path)
        return [txt for txt in txt_files if txt[-4:] == ".txt" and int(txt[:-4]) not in self.products_loaded]  # Only .txt files
    
    def read_txt(self) -> None:
        for txt in self.tickets_txt:
            print("ticket", txt)
            path_to_txt = os.path.join(pdf_path, f"{txt}")
            with open(f"{path_to_txt}") as ticket:
                t = [line for line in ticket]
            parsed = self.get_products(t)
            posted = self.post_to_db(parsed, txt[:-4])
            if posted:
                self.errors.append(posted)

    # parse products data from tickets
    def get_products(self, ticket_parsed) -> dict():
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
            product.append(line[5:25])
            pvp.append(line[26:32])
            total.append(line[32:38])
        
        parsed["quantity"] = quantity
        parsed["product"] = product
        parsed["pvp"] = pvp
        parsed["total"] = total

        return parsed

    # load to db
    def post_to_db(self, parsed_products, ticket_id) -> list():
        errors = list()  # failed statements

        for n in range(len(parsed_products["product"])):
            values = list()
            values.append(ticket_id)

            for columns in parsed_products.keys():
                value = parsed_products[columns][n].replace(",", ".").strip().replace("'", "")
                if (value == "" or value == "-"):
                    value = None
                values.append(value)

            try:
                db.prepare_conn()
                db.cur.execute(sql.SQL("INSERT INTO {} VALUES (%s, %s, %s, %s, %s)").format(sql.Identifier("products")), values)
                db.conn.commit()
            except err as error:
                errors.append(error)
                db.conn.rollback()
            finally:
                db.close()

        return errors if errors else None
    
    # post errors to .txt
    def post_errors(self) -> None:
        timestamp = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(time.time()))
        if self.errors:
            with open(Path(f"consum/consum_project/data/{timestamp}_errors.txt"), "w") as file:
                for e in self.errors:
                    print(e)
                    file.write(str(e[0]))
                print(f"{len(self.errors)} errors added to the log")


# instantiate objects for calling from DAG
def scrapper() -> None:
    consum = Page()
    consum.scrap()

def ticket_parser() -> None:
    parser = TicketParser()
    parser.read_txt()
    parser.post_errors()


if __name__ == '__main__':
    # Scrape consum page to retrieve tickets
    consum = Page()
    consum.scrap()

    # Parse the tickets and insert to db
    parser = TicketParser()
    parser.read_txt()
    parser.post_errors()
