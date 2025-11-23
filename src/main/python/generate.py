import random
import datetime

output = "orders_50gb.xml"

def write_line(f, text, indent=0):
    f.write(" " * indent + text + "\n")

# ------------------------------
# TUNABLE PARAMETERS FOR SIZE
# ------------------------------
NUM_ORDERS = 5000        # Increase to reach 50 GB
ITEMS_PER_ORDER = 10000            # average number of items
TAGS_PER_ORDER = 10000             # tags per order

# For reproducible randomness
random.seed(1234)

# Example product list
PRODUCTS = [
    ("Wireless Mouse", "Electronics", 29.99),
    ("USB Cable", "Electronics", 9.99),
    ("Laptop Stand", "Accessories", 49.99),
    ("Keyboard", "Electronics", 79.99),
    ("Monitor", "Electronics", 299.99),
    ("Headphones", "Electronics", 199.99),
    ("Coffee Maker", "Appliances", 89.99),
    ("Desk Lamp", "Furniture", 34.99),
    ("Gaming Chair", "Furniture", 349.99),
    ("Desk Mat", "Accessories", 19.99),
]

def random_date():
    base = datetime.date(2025, 11, 1)
    return base + datetime.timedelta(days=random.randint(0, 30))

def random_price(base):
    return round(base + random.random() * 20, 2)

with open(output, "w", encoding="utf-8") as f:

    write_line(f, '<?xml version="1.0" encoding="UTF-8"?>')
    write_line(f, "<Orders>")

    for o in range(NUM_ORDERS):

        order_id = 10000 + o
        order_date = random_date().isoformat()
        status = random.choice(["Shipped", "Delivered", "Processing", "Cancelled"])

        # --------------------
        # ORDER START

        write_line(f, f'<Order OrderID="{order_id}" OrderDate="{order_date}" Status="{status}">', 2)

        # CUSTOMER
        cust_id = f"C{o:05}"
        write_line(f, f'<Customer CustomerID="{cust_id}" Name="Customer{o}" Email="customer{o}@example.com" MemberSince="2020-01-01"/>', 4)

        # SHIPPING ADDRESS
        write_line(f, "<ShippingAddress>", 4)
        write_line(f, f"<Street>{o} Main Street</Street>", 6)
        write_line(f, "<City>Cityville</City>", 6)
        write_line(f, "<State>CA</State>", 6)
        write_line(f, "<ZipCode>90000</ZipCode>", 6)
        write_line(f, "<Country>USA</Country>", 6)
        write_line(f, "</ShippingAddress>", 4)

        # BILLING ADDRESS (same for simplicity)
        write_line(f, "<BillingAddress>", 4)
        write_line(f, f"<Street>{o} Main Street</Street>", 6)
        write_line(f, "<City>Cityville</City>", 6)
        write_line(f, "<State>CA</State>", 6)
        write_line(f, "<ZipCode>90000</ZipCode>", 6)
        write_line(f, "<Country>USA</Country>", 6)
        write_line(f, "</BillingAddress>", 4)

        # ORDER ITEMS
        write_line(f, "<OrderItems>", 4)

        for i in range(ITEMS_PER_ORDER):
            pid = f"P{o}{i:04}"
            name, category, base_price = random.choice(PRODUCTS)
            qty = random.randint(1, 5)
            price = random_price(base_price)

            write_line(
                f,
                f'<Item ItemID="{pid}" ProductName="{name}" Category="{category}" Quantity="{qty}" UnitPrice="{price}">', 
                6
            )

            # Discount section
            if random.random() < 0.5:
                write_line(f, '<Discount Type="Percentage">10</Discount>', 8)
            else:
                write_line(f, '<Discount Type="Fixed">5</Discount>', 8)

            # Tax
            tax_amount = round(price * qty * 0.1, 2)
            write_line(f, f"<Tax>{tax_amount}</Tax>", 8)

            write_line(f, "</Item>", 6)

        write_line(f, "</OrderItems>", 4)

        # PAYMENT
        write_line(
            f,
            f'<Payment PaymentMethod="CreditCard" CardType="Visa" LastFourDigits="{random.randint(1000,9999)}" Amount="{round(price*qty,2)}"/>',
            4
        )

        # NOTES
        write_line(f, "<Notes>Auto-generated order</Notes>", 4)

        # TAGS
        write_line(f, "<Tags>", 4)
        for t in range(TAGS_PER_ORDER):
            write_line(f, f"<Tag>tag{t}</Tag>", 6)
        write_line(f, "</Tags>", 4)

        # ORDER END
        write_line(f, "</Order>", 2)

    write_line(f, "</Orders>")
