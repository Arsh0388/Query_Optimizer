import csv
import sqlite3
import random

global c1, c2, c3
conn1 = sqlite3.connect('A3small.db')
c1 = conn1.cursor()
c1.execute('PRAGMA foreign_keys=ON;')
conn2 = sqlite3.connect('A3medium.db')
c2 = conn2.cursor()
c2.execute('PRAGMA foreign_keys=ON;')
conn3 = sqlite3.connect('A3large.db')
c3 = conn3.cursor()
c3.execute('PRAGMA foreign_keys=ON;')
creation = '''CREATE TABLE IF NOT EXISTS "Customers" ( "customer_id" TEXT,"customer_postal_code" INTEGER,PRIMARY KEY("customer_id") );
CREATE TABLE IF NOT EXISTS "Sellers" ( "seller_id" TEXT,"seller_postal_code" INTEGER,PRIMARY KEY("seller_id") );
CREATE TABLE IF NOT EXISTS  "Orders" ( "order_id" TEXT,"customer_id" TEXT,PRIMARY KEY("order_id"),FOREIGN KEY("customer_id") REFERENCES "Customers"("customer_id"));
CREATE TABLE IF NOT EXISTS  "Order_items" ( "order_id" TEXT,"order_item_id" INTEGER,"product_id" TEXT,"seller_id" TEXT,PRIMARY KEY("order_id","order_item_id","product_id","seller_id"), FOREIGN KEY("seller_id") REFERENCES "Sellers"("seller_id") FOREIGN KEY("order_id") REFERENCES "Orders"("order_id"));'''
c1.executescript(creation)
c2.executescript(creation)
c3.executescript(creation)
conn1.commit()
conn2.commit()
conn3.commit()

def populating_sellers(cardinality):
    with open('olist_sellers_dataset.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"')
        header = next(reader)

        seller_id = []
        seller_zip_code_prefix = []
        seller_city = []
        seller_state = []
        i = 0

        for row in reader:
            seller_id.append(row[0])
            seller_zip_code_prefix.append(row[1])
            seller_city.append(row[2])
            seller_state.append(row[3])
            i += 1

        if cardinality == 500:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                print(f"{seller_id[number]}, {seller_zip_code_prefix[number]}")
                c1.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (seller_id[number], seller_zip_code_prefix[number]))

        elif cardinality == 750:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c2.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (seller_id[number], seller_zip_code_prefix[number]))

        else:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c3.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (seller_id[number], seller_zip_code_prefix[number]))

        conn1.commit()
        conn2.commit()
        conn3.commit()


def populating_customers(cardinality):
    with open('olist_customers_dataset.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"')
        header = next(reader)

        customer_id = []
        customer_unique_id = []
        customer_zip_code_prefix = []
        customer_city = []
        customer_state = []

        i = 0

        for row in reader:
            customer_id.append(row[0])
            customer_unique_id.append(row[1])
            customer_zip_code_prefix.append(row[2])
            customer_city.append(row[3])
            customer_state.append(row[4])
            i += 1

        if cardinality == 10000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                print(f"{customer_id[number]}, {customer_zip_code_prefix[number]}")
                c1.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (customer_id[number], customer_zip_code_prefix[number]))

        elif cardinality == 20000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c2.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (customer_id[number], customer_zip_code_prefix[number]))

        else:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c3.execute(f"INSERT INTO Sellers ({','.join(header)}) VALUES (?, ?)", (customer_id[number], customer_zip_code_prefix[number]))

        conn1.commit()
        conn2.commit()
        conn3.commit()

def populating_Orders(cardinality):
    with open('olist_orders_dataset.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"')
        header = next(reader)
        order_id = []
        customer_id = []

        i = 0

        for row in reader:
            order_id.append(row[0])
            customer_id.append(row[1])

            i += 1

        if cardinality == 10000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                # print(f"{customer_id[number]}, {customer_zip_code_prefix[number]}")
                c1.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], customer_id[number]))

        elif cardinality == 20000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c2.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], customer_id[number]))

        else:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c3.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], customer_id[number]))

        conn1.commit()
        conn2.commit()
        conn3.commit()

def populating_orders_items(cardinality):
    with open('olist_orders_dataset.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"')
        header = next(reader)
        order_id = []
        order_item_id = []
        product_id = []
        seller_id = []


        i = 0

        for row in reader:
            order_id.append(row[0])
            order_item_id.append(row[1])
            product_id.append(row[2])
            seller_id.append(row[3])

            i += 1

        if cardinality == 10000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                # print(f"{customer_id[number]}, {customer_zip_code_prefix[number]}")
                c1.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], order_item_id[number],product_id[number],seller_id[number]))

        elif cardinality == 20000:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c2.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], order_item_id[number],product_id[number],seller_id[number]))

        else:
            for j in range(cardinality):
                number = random.randint(0, i-1)
                c3.execute('''INSERT INTO Customers VALUES (?, ?)''', (order_id[number], order_item_id[number],product_id[number],seller_id[number]))

        conn1.commit()
        conn2.commit()
        conn3.commit()

def main() :
    populating_sellers(500)
    populating_sellers(750)
    populating_sellers(1000)
    populating_customers(10000)
    populating_customers(20000)
    populating_customers(330000)
    populating_Orders(10000)
    populating_Orders(20000)
    populating_Orders(33000)
    populating_orders_items(2000)
    populating_orders_items(4000)
    populating_orders_items(10000)