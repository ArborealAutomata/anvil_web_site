# -*- coding: utf-8 -*-
"""
Created on Sat May 13 06:52:11 2023

@author: jdavis
"""

import anvil.server
import pyodbc
import pandas as pd
import random
import os
import datetime
import warnings
import sqlite3

warnings.filterwarnings("ignore")

SET_USERNAME = 'username'
DISCOUNT = 0.1
DISCOUNT_MAX = 0.4

# connect to the anvil server.
anvil.server.connect("QGRYRE5DARUFNQHIOKHJIM3N-EJG5YUEN7FX3NVX2")
random.seed()

# find the current directory that this file is being executed in.
CURRENT_DIR = os.getcwd()
DATA_PATH = CURRENT_DIR +'/data'

def open_db_connect():
    '''
    connect to the database: raise exception if cannot connect
    requires a database caleld flight_booking_system
    CREATE DATABASE flight_booking_system;
    '''
    try:

        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LAPTOP-O7O00DNK\SQLEXPRESS;'
                              'Database=flight_booking_system;'
                              'Trusted_Connection=yes;')
        return conn
    except ConnectionError as exc:
        raise RuntimeError('Failed to open database') from exc

def create_sqlite_db():
    '''
    creates a database called: "flight_booking_system.db"
    if it does not exist sqlite.connect() will create one
    in the folder "data".
    Returns the connection to the database.
    '''
    db_name = "flight_booking_system.db"
    db_path = os.path.join(DATA_PATH,db_name)
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as exc:
        raise RuntimeError(f"Failed to open database {exc}") from exc

def read_db_query(read_command):
    # takes in a command, opens the database, executes the command.
    try:
        # create a database connection to read from
        conn = open_db_connect()
        # read the query from the database
        data = pd.read_sql_query(read_command, conn)
        # close the connection.
        conn.close()
        return data
    except ConnectionError as exc:
        conn.close()
        raise RuntimeError('Failed to open database') from exc


def write_db_query(action):
    try:
        # create a database connection to write to
        conn = open_db_connect()
        # create curser
        cursor = conn.cursor()
        # execute querey command.
        cursor.execute(action)
        conn.commit()
    except ConnectionError as exc:
        raise RuntimeError('Failed to execute db query: '+ action) from exc
    # close the connection.
    conn.close()
     

def random_numbers(number):
    # returns a list of string number random numbers 0-9 as
    try:
        return [str(random.randint(0,9)) for i in range(number)]
    except:
        return 0

    

def random_letters(number):
    # returns a list of number random alphabet characters
    try:
        return [random.choice('abcdefghijklmnopqrstuvwxyz')for x in range(number)]
    except:
        return None

def initialize_system():
    '''
    check if the following datatables exist:
        flight_seats
        user_orders
        customer_feedback
    if they do not, then create them.
    
    try and connect to customer_feedback table:
      if not found, create a new one in the database
    try and connect to flight_seats table:
      if not found, create a new one in the database
    try and connect to user_orders table:
      if not found, create a new one in the database
    if the flight_seats is empty
      then populate the datatable with initial options
      with an initial setup of:
        economy_tickets : 100 : 100
        premium_tickets : 60  : 200
        first_class_tickets : 20 : 300
    '''
    # create a data directory to save files to
    # if it does not exist.
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
        print(f"{DATA_PATH} has been created")
    file_path = os.path.join(DATA_PATH, "password.txt")
    
    
    # check customer_feedback
    action = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'customer_feedback'"
    feedback = read_db_query(action)
    if feedback.empty is True:
        action = (f"""CREATE TABLE customer_feedback (
                    feedback_code INT PRIMARY KEY,
                    customer_name VARCHAR(255),
                    ticket_order_code VARCHAR(255),
                    feedback TEXT,
                    response_left TEXT
                    );"""
                )
        write_db_query(action)

    # check flight_seats
    action = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'flight_seats'"
    flight = read_db_query(action)
    if flight.empty is True:
        action = (f"""CREATE TABLE flight_seats (
                    type_of_seat VARCHAR(255) PRIMARY KEY,
                    num_of_free_seats INT,
                    price_per_ticket INT);"""
                 )
        write_db_query(action)
        
        
    # check user_orders
    action = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'user_orders'"
    orders = read_db_query(action)
    if orders.empty is True:
        action = (f"""CREATE TABLE user_orders 
                  (ticket_order_code VARCHAR(255) PRIMARY KEY,
                   customer_name VARCHAR(255),
                   number_of_tickets_ordered INT,
                   economy_tickets INT,
                   first_class_tickets INT,
                   premium_tickets INT,
                   discount_applied FLOAT,
                   price_per_order FLOAT
                   );"""
                  )
        write_db_query(action)

    
    if flight.empty:
        seat_init = {'type_of_seat' : ['economy_tickets',
                                       'first_class_tickets',
                                       'premium_tickets'
                                       ],
                     'num_of_free_seats' : [100, 20, 60],
                     'price_per_ticket' : [100, 300, 200]
                     }
        keys = list(seat_init.keys())
        items = len(seat_init.values())
        for i in range(items):
            data = [str(seat_init[keys[0]][i]), seat_init[keys[1]][i], seat_init[keys[2]][i]]
            
            action = (f"""INSERT INTO flight_seats(
                        type_of_seat, 
                        num_of_free_seats,
                        price_per_ticket
                        ) VALUES (
                                \'{data[0]}\',\'{data[1]}\',
                                \'{data[2]}\'
                                );"""
                     )

            write_db_query(action)
    # generate a unique admin_password
    # that saves into a .txt in the location
    # that this code is executed in
    
    if os.path.exists(file_path) and os.path.isfile(file_path):
        pass
    else:
        # generate a list of 30 random letters.
        letters = random_letters(30)
        # pick 30 of them at random and capotalise them
        data = [random.choice(letters).upper() for i in range(30)]
        # combine both lists with 30 random numbers then scramble them
        data += letters + random_numbers(30)
        random.shuffle(data)
        
        # create a file, write the 90 characters, then truncate to 12.
        file = open(file_path,"w")
        file.writelines(data)
        file.close()
    
        file = open(file_path,"a")
        file.truncate(12)
        file.close()


def generate_booking_id():
    # create unique booking id
    booking_id = 0
    # create a list of 6 random numbers
    code ="".join(random_numbers(6))    
    # get a list of 6 random letters
    data = random_letters(6)
    # make the first 3 capitals
    letters = [random.choice(data).upper() if i%2 else random.choice(data).lower() for i,j in enumerate(data)]
    
    caps = [letters.pop(i) for i,j in enumerate(letters) if j.isupper() is True]
    # put the first 3 infront of the numbers
    # put the last 3 behind the numbers
    #join them all together
    booking_id = "".join(caps) +code+"".join(letters)
    return booking_id


@anvil.server.callable
def admin_login_check(admin_name, admin_password):
    try:
        file_path = os.path.join(DATA_PATH, "password.txt")
        file = open(file_path,"r")
        password = file.read()
        file.close()
        if admin_name== SET_USERNAME and admin_password == password:
            return True
        else:
            return False
    except:
        return False


@anvil.server.callable
def update_tickets(tickets_passed):
    #read the current number of tickets
    # tickets_passed should be a list of 3 int values

    try:
        seats = seat_availability_check()
        # filter the tickets_passed to only have seat types 'economy' etc by getting
        # the seat types from the seats 
        
        tickets_to_add = {i: int(tickets_passed[i]) for i in tickets_passed if i in seats} 

        # ensure all values in both are int
        for i in list(seats.keys()):
            tickets_to_add[i] = -int(tickets_to_add[i])
            seats[i] = int(seats[i])
        
        #add the new ticket
        new_tickets = [sum(i) for i in zip(list(seats.values()), list(tickets_to_add.values()))]

        #overwrite the existing ticket values
        new_ticket_dict = dict(zip(list(seats.keys()), new_tickets))

        for key, value in new_ticket_dict.items():
            # for each key overwrite the corrisponding value in the datatable
            command = (f"""UPDATE flight_seats 
                       SET num_of_free_seats= \'{value}\'
                       WHERE type_of_seat =\'{key}\';"""
                       )
            write_db_query(command)
            
    except AttributeError as exc:
        raise RuntimeError('add_tickets: missing arguments') from exc

def read_flight_seat_numbers():
    return read_db_query('SELECT * FROM flight_seats')


@anvil.server.callable
def seat_availability_check(seat_type=None, number=None):
    '''
    Pass seat type and number of seats
    If the function is being used to check
    seat availability:
        if no arguments are passed, 
            then it returns a dictionary of seat types with
            their corrisponding values.
        if a string seat type, int number pair is passed 
            it checks to see if there is there are more or equal to the specified
            number of that teat type available.
        if two lists are passed:
            does as above but for a list of seat type: number pairs.
            
    '''
    #get the number of ticket seat types
    seats_db = read_flight_seat_numbers()
            
    if isinstance(seat_type, str) and isinstance(number, int):
        # check if there are enough free seats
        free_seats = int(seats_db.loc[seats_db['type_of_seat'] == seat_type,'num_of_free_seats'])
        if free_seats >= number:
            return True
        else:
            return False
        
    elif isinstance(seat_type, list) and isinstance(number, list):
        for i, type_of_seat in enumerate(seat_type):
            free_seats = int(seats_db.loc[seats_db['type_of_seat'] == type_of_seat,'num_of_free_seats'])
            if number[i] > free_seats:
                return False
        return True
    
    elif seat_type == None and number == None:
        # values of the types of seat
        seat_types = [i for i in seats_db['type_of_seat']]
        # list of free_seats found for each type of seat
        free_seats = [int(seats_db.loc[seats_db['type_of_seat'] == i,'num_of_free_seats']) for i in seat_types]
        return dict(zip(seat_types, free_seats))
    else:
        return False


@anvil.server.callable
def calculate_cost(tickets):
    #tickets type dict
    total_tickets = 0
    #testing to capture any data type, also to sum the tickets 
    #passed in to calculate the dicount.
    if isinstance(tickets, dict):
        total_tickets = sum([int(i) for i in tickets.values()])
    elif isinstance(tickets, list):
        total_tickets = sum([int(i) for i in tickets])
    elif isinstance(tickets, int):
        total_tickets = tickets
    elif isinstance(tickets, str):
        try:
            total_tickets = int(tickets)
        except:
            return 0
    
    if total_tickets == 2:
        discount = DISCOUNT
    if total_tickets < 2:
        discount = 0
    if total_tickets > 2:
        discount = round((total_tickets/50 + DISCOUNT),2)
        if discount > DISCOUNT_MAX:
            discount = DISCOUNT_MAX
            
    # add up the cost of the total number of tickets
    seats_db = read_flight_seat_numbers()
    seat_types = [i for i in seats_db['type_of_seat']]
    
    seat_prices = [int(seats_db.loc[seats_db['type_of_seat'] == i,'price_per_ticket']) for i in seat_types]
    cost = sum([seat_prices[i]*tickets[seat_type] for i,seat_type in enumerate(seat_types)])
    # apply discount
    if total_tickets <= 0:
        cost = 0
    cost -= cost*discount
    return cost, discount


@anvil.server.callable
def flight_details(booking_code, customer_name):
    #looks up the user's flight details based on the unique ticket code and name
    try:
        query = (f"""SELECT * 
                 FROM user_orders 
                 WHERE ticket_order_code = \'{booking_code}\';"""
                 )
        booking_details = read_db_query(query)
        booking_details = dict(booking_details.loc[0])
        if booking_details != None:
            return booking_details
        else:
            return None
    except:
        return None


@anvil.server.callable
def book_flight(customer_name, tickets):
    # check the availability of the tickets
    
    if seat_availability_check(list(tickets.keys()), list(tickets.values())) is True:
        
        cost, discount = calculate_cost(tickets)
        
        total_tickets = sum(tickets.values())
        economy = tickets['economy_tickets']
        first_class = tickets['first_class_tickets']
        premium = tickets['premium_tickets']
        booking_id = ''
        while True:
            booking_id = generate_booking_id()
            if flight_details(booking_id, customer_name) is None:
                break
        # store users order details in the database
        update_tickets(tickets)
        query = (f"""INSERT INTO user_orders (
                ticket_order_code, customer_name, 
                number_of_tickets_ordered, economy_tickets, 
                first_class_tickets, premium_tickets,  
                discount_applied, price_per_order
                ) 
                VALUES (
                     \'{booking_id}\',\'{customer_name}\',
                     \'{total_tickets}\',\'{economy}\',
                     \'{first_class}\',\'{premium}\',
                     \'{discount}\',\'{cost}\'
                     );"""
                )
        write_db_query(query)
        
    return booking_id, total_tickets, cost, discount



@anvil.server.callable
def cancel_ticket(order_code, customer_name):
    # check if the customer's order exists
    booking = flight_details(order_code, customer_name)
    # breaks out triggering a popup to display the booking
    # does not exist
    if booking is None:
        return False
    #replace the tickets back into the database.

    if booking.get('economy_tickets') == None:
        booking['economy_tickets'] = 0 
    if booking.get('first_class_tickets') == None:
        booking['first_class_tickets'] = 0

    if booking.get('premium_tickets') == None:
        booking['premium_tickets'] = 0
        
    booking['economy_tickets'] = -int(booking['economy_tickets'])
    booking['first_class_tickets'] = -int(booking['first_class_tickets'])
    booking['premium_tickets'] = -int(booking['premium_tickets'])
    
    update_tickets(booking)
    # look up the line in the database and delete
    delete_order = (f"""DELETE FROM user_orders 
                    WHERE ticket_order_code =\'{order_code}\' 
                    AND customer_name =\'{customer_name}\';"""
                    )
    
    write_db_query(delete_order)
    return True


@anvil.server.callable
def feedback_call(feedback_code = None):
    #feedback = read_db_query(read_command)
    # look up a piece of piece of feedback if a code is passed to it
    # or returns all the feedback.
    try:
        if feedback_code != None:
            read_command = (f"""SELECT * FROM customer_feedback WHERE feedback_code = \'{feedback_code}\'"""
                            )
        else:
            read_command = f'SELECT * FROM customer_feedback'
        feedback = read_db_query(read_command)
        return feedback
    except:
        return None


@anvil.server.callable
def user_leave_feedback(customer_name, feedback, ticket_order_code = ''):
    # takes in the user's feedback with their name and ticket order code
    # assign a number to the feedback, create an empty cell to respond to feedback
    # add to the feedback database

    feedback_code = 0
    while True:
        # create feedback code and check if unique
        feedback_code = ''.join(random.choices(random_numbers(8), k=8))
        if get_feedback_booking_code(feedback_code) is not None:
            break
    
    response = None
    write_command = (f"""INSERT INTO customer_feedback 
                     (feedback_code, 
                      customer_name, 
                      ticket_order_code, 
                      feedback, 
                      response_left
                      )
                     VALUES (
                            {feedback_code},
                            \'{customer_name}\',
                            \'{ticket_order_code}\',
                            \'{feedback}\',\'{response}\'
                            );"""
                    )
    write_db_query(write_command)
    return feedback_code


@anvil.server.callable
def user_get_response(feedback_code, customer_name = None):
    #using the user supplied feedback code,
    # looks up a piece of piece of feedback
    # and also the admin generated response left from the database
    # returns these.

    read_command = (f"""SELECT feedback 
                    FROM customer_feedback 
                    WHERE feedback_code = \'{feedback_code}\'"""
                    )
    feedback = read_db_query(read_command)
    feedback = feedback['feedback'].to_string(index=False)
    #feedback
    read_command = (f"""SELECT response_left 
                    FROM customer_feedback 
                    WHERE feedback_code = \'{feedback_code}\'"""
                    )
    response = read_db_query(read_command)
    response = response['response_left'].to_string(index=False)
    
    if response == None:
        response = ''
    
    return feedback, response



@anvil.server.callable
def admin_view_feedback():
    # read in all feedback.
    all_feedback = feedback_call()
    # discriminating that which does not have a resposce 
    # those that can have a response left are flagged 
    # others marked as answered
    read_command = (f"""SELECT * 
                    FROM customer_feedback 
                    WHERE 
                    CONVERT(VARCHAR(MAX), response_left) = \'None\';"""
                    )
    unanswered = read_db_query(read_command)
    
    read_command = (f"""SELECT * 
                    FROM customer_feedback 
                    WHERE 
                    CONVERT(VARCHAR(MAX), response_left) != \'None\';"""
                    )
    answered = read_db_query(read_command)
    # pull out just the 'feedback_code' and 'customer_name'
    # turn from dataframe to list of 'feedback_code', 'customer_name' pairs
    # of the format ['feedback_code', 'customer_name']
    # do this for both ansered and unanswered
    ans_names = answered[['feedback_code', 'customer_name']]
    unans_names = unanswered[['feedback_code', 'customer_name']]
    
    answered_feedback = ans_names.values.tolist()
    unanswered_feedback = unans_names.values.tolist()
    
    return answered_feedback, unanswered_feedback

@anvil.server.callable
def get_feedback_booking_code(feedback_id, customer_name = ''):
    # look up the booking code for looking up in admin feedback
    try:
        read_command = (f"""SELECT ticket_order_code 
                        FROM customer_feedback 
                        WHERE feedback_code = \'{feedback_id}\'"""
                        )
        booking_code = read_db_query(read_command)
        if booking_code is not None:
            booking_code = booking_code['ticket_order_code'].to_string(index=False)
        else:
            return None
        return booking_code
    except:
        return None

@anvil.server.callable
def admin_feedback_respose(feedback_id, customer_name, response):
    # save a response to the feedback
    # 
    try:
        command = (f"""UPDATE customer_feedback 
                   SET response_left = \'{response}\' 
                   WHERE feedback_code = \'{feedback_id}\' 
                   AND customer_name = \'{customer_name}\';"""
                   )
        write_db_query(command)
    except:
        pass



@anvil.server.callable
def generate_report():
    
    #generate reports showing:
        #number of tickets booked per type, 
        #total tickets booked, 
        #income made per ticked type, 
        #total income made. 
    
    # get the path of the current directory and time & date
    
    date = datetime.datetime.now()
    timestamp = date.strftime("%Y%m%d_%H%M%S")
    # create a timestamped file name.
    filename = 'report_' + timestamp +'.csv'
    csv_path = os.path.join(DATA_PATH,filename)
    
    # read from flight_seats the type of seats and number of seats
    validated_view = (f"""CREATE VIEW validated_feedback AS
                      SELECT *
                      FROM customer_feedback
                      WHERE ticket_order_code IS NOT NULL;"""
                      )
    
 
    
    feedback_view = ("""CREATE VIEW user_orders_with_feedback AS
                        SELECT user_orders.ticket_order_code, 
                               user_orders.customer_name, 
                               user_orders.number_of_tickets_ordered,
                               user_orders.economy_tickets,  
                               user_orders.first_class_tickets,
                               user_orders.premium_tickets,
                               user_orders.discount_applied, 
                               user_orders.price_per_order, 
                               customer_feedback.feedback_code, 
                               customer_feedback.feedback, 
                               customer_feedback.response_left
                        FROM user_orders
                        LEFT JOIN customer_feedback 
                        ON user_orders.ticket_order_code = customer_feedback.ticket_order_code
                        AND user_orders.customer_name = customer_feedback.customer_name;"""
                     )
    #print(feedback_view)
    
    # read from user_orders all orders
    # do not read in customer_name custom view
    read_command = (f"""SELECT number_of_tickets_ordered, economy_tickets, 
                    first_class_tickets, premium_tickets,  
                    price_per_order 
                    FROM user_orders;"""
                    )
    report_view = read_db_query(read_command)
    # generate a temporary table displaying the 
    
    report_view.fillna(0, inplace=True)
    report_view[['number_of_tickets_ordered', 'economy_tickets', 
                 'first_class_tickets', 'premium_tickets', 'price_per_order'
                 ]]

    # calculate the earnings per ticket type: ticket_type / total_tickets * total earnings
    # 

    columns = ['economy_tickets', 'first_class_tickets', 'premium_tickets']
    new_column_names = ['economy_ticket_earnings', 'first_class_ticket_earnings', 'premium_ticket_earnings']
    for i, colnames in enumerate(columns):
        report_view[new_column_names[i]] = ((report_view[colnames] / report_view['number_of_tickets_ordered'])
                                            * report_view['price_per_order'])
        report_view[new_column_names[i]] = ((report_view[colnames] / report_view['number_of_tickets_ordered'])
                                            * report_view['price_per_order'])
        report_view[new_column_names[i]] = ((report_view[colnames] / report_view['number_of_tickets_ordered'])
                                            * report_view['price_per_order'])
        


    report_view.rename(columns = {'number_of_tickets_ordered' : 'total_number_of_tickets_sold', 
                                  'economy_tickets' : 'total_economy_tickets_sold',
                                  'first_class_tickets' : 'total_first_class_tickets_sold',
                                  'premium_tickets' : 'total_premium_tickets_sold', 
                                  'price_per_order' : 'total_earnings' 
                                 }, inplace=True)
    
    
    op_frame = pd.DataFrame(report_view.sum(),columns=['sum_totals']).rename_axis('field_names').reset_index()
    # print to csv
    op_frame.to_csv(csv_path)
    
    
initialize_system()

generate_report()

def test_run():
    asd_alice = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[2,1,1]))
    asd_bob = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[10,1,6]))
    asd_char = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[6,0,0]))
    asd_dave = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[0,2,0]))
    asd_est = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[0,0,3]))
    asd_frank = dict(zip(['economy_tickets','first_class_tickets','premium_tickets'],[50,0,10]))
    
    names = ['Alice', 'Bob', 'Charlie', 'Dave', 'Ester', 'Frank']
    orders = [asd_alice, asd_bob, asd_char, asd_dave, asd_est, asd_frank]
    
    feedback = ['Your website is really good!',  
                'You do not have enough tickets for my needs', 
                'Thank you', 
                "This webiste is better than anyone else's", 
                "Congratulations! You've won a $1000 Walmart gift card. Go to ow website tp claim now!",
                "Amazon is sending you a refunding of $32.64. Please reply with your bank account and routing number ro recieve your refund."
                ]
    responses = ["Hello, thank you for your feedback.",
                 "Hello, more tickets have alreay been made available.",
                 "Thank you."
                ]
    book_id = 0
    booking_id_code = []
    total_tickets = 0
    cost = 0
    discount = 0
    feedback_codes = []
    
    for i, order in enumerate(orders):
        book_id, total_tickets, cost, discount = book_flight(names[i], order)  # return booking_id, total_tickets, cost, discount
        booking_id_code.append(book_id)
    
    for i, name in enumerate(names):
        feedback_codes.append(user_leave_feedback(name, feedback[i], booking_id_code[i%2])) # return feedback_code
    
    for i, answer in enumerate(responses):
        admin_feedback_respose(feedback_codes[i], names[i], answer)
        
#test_run()
