CREATE TABLE IF NOT EXISTS tickets (
    id INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS products (
    ticket_id INT NOT NULL,
    quantity FLOAT,
    product VARCHAR (50),
    pvp FLOAT,
    total FLOAT,
    CONSTRAINT fk_ticket
        FOREIGN KEY(ticket_id)
            REFERENCES tickets(id)
);
