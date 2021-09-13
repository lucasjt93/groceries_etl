""" Scraps the consum web page to download tickets as PDF files to a local folder """

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

import time
import csv
import os
from pathlib import Path

import configparser


class Page:
    def __init__(self):
        self.driver = None
        self.tickets = list()
        self.stored = list()
        self.stored_path = Path("consum_project/data/tickets.csv")
        self.pdf_path = Path("consum_project/data/tickets_pdf")

    @staticmethod
    def timer():
        return time.time()

    # get stored tickets from csv
    def stored_tickets(self):
        if self.stored_path.exists():
            with open(self.stored_path, mode="r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    self.stored.append(row[0])
                    self.tickets.append(row[0])

    # post new tickets to stored files
    def new_tickets(self):
        with open(self.stored_path, mode="a", encoding="utf-8-sig", newline="") as f:
            for ticket in self.tickets:
                if ticket not in self.stored:
                    f.write(ticket + "\n")
        print(f"Database updated with {len(self.tickets) - len(self.stored)} tickets")

    # get webdriver for chrome
    def get_driver(self):
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
    def go_to_page(self):
        self.driver.get("https://mundoconsum.consum.es/auth/index")
        assert "Consum" in self.driver.title

        # accept cookies
        button = self.driver.find_element_by_id("onetrust-accept-btn-handler")
        ActionChains(self.driver).move_to_element(button).click(button).perform()

    # login using credentials from config
    def login(self):
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
    def rename_file(self, ticket_id):
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
    def download_ticket(self, ticket_id):
        menu = self.driver.find_element_by_id("menu-puntos")
        ActionChains(self.driver).move_to_element(menu).click(menu).perform()
        download = self.driver.find_element_by_id("dropdown1")
        ActionChains(self.driver).move_to_element(download).click(download).perform()
        print(f"ticket {ticket_id} downloaded")
        self.rename_file(ticket_id)
        self.driver.back()  # go back to tickets list

    def get_tickets(self):
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
                if ticket_id not in self.tickets:  # check that ticket is not yet scrapped
                    self.tickets.append(ticket_id)
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
    def teardown(self):
        self.driver.close()

    def scrap(self):
        # lets take the time
        start_time = self.timer()
        print(f"Started at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(start_time))}")

        # get the driver ready + configs
        self.get_driver()
        self.stored_tickets()

        # scrapes the page
        self.go_to_page()
        self.login()
        self.get_tickets()

        # add new tickets to the csv file
        self.new_tickets()

        # close the driver
        self.teardown()

        # stop timer
        end_time = self.timer()

        print(f"Finalized at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(end_time))}")
        print(f"Total time: {time.strftime('%M:%SZ', time.localtime(end_time - start_time))}")

# instantiate object for calling from DAG
def scrapper():
    consum = Page()
    consum.scrap()


if __name__ == '__main__':
    """ Scrape consum page to retrieve tickets """

    consum = Page()
    consum.scrap()
