from tabulate import tabulate
import mysql.connector as bigS
import math
import googlemaps
from datetime import datetime
import pandas
gmaps = googlemaps.Client(key='AIzaSyC3Tb9A0TvsYGy2uccaVoVMq6CTQenv8_A')
database="monogamyv3"
password="Ganesha123"
mycon=bigS.connect(host="localhost", user="root",password=password,database=database)
cur=mycon.cursor()
table_names = {"orders":['status','buyer_name','seller_name','delivery_dude_name','food','price'],"menu":['seller_name','food_type','food_quantity','food_price'],"buyers":['username','password','location_lat','location_lon','address'], "sellers":['username','password','location_lat','location_lon','address'],"delivery_dude":['username','password','location_lat','location_lon']}
#cur.execute("create database",database,";")
def create_table():
        #creating the table structure in the database
    #try:
        if mycon.is_connected():
            #cur.execute("use",database)
            tables = {}
            tables['Buyer'] = ("create table Buyers (username varchar(50) not null primary key, password varchar(20), location_lat float(20,10), location_lon float(20,10), address varchar(255));")
            tables['Seller'] = ("create table Sellers (username varchar(50) not null primary key, password varchar(20), location_lat float(20,10), location_lon float(20,10), address varchar(255));")
            tables['Delivery_Dude'] = ("create table Delivery_Dude (username varchar(50) not null primary key, password varchar(20), location_lat float(20,10), location_lon float(20,10));")
            tables['Orders'] = ("create table Orders (status int(1), buyer_name varchar(50), seller_name varchar(50), delivery_dude_name varchar(50), food varchar(255), food_price int, foreign key(seller_name) references Sellers(username),foreign key(buyer_name) references Buyers(username),foreign key(delivery_dude_name) references Delivery_Dude(username));")
            tables['Menu'] = ("create table Menu (seller_name varchar(50), food_type varchar(20), food_quantity int, food_price int, foreign key(seller_name) references Sellers(username) );")
        for i in tables:
            print(tables[i])
            
            cur.execute(tables[i])
        print(cur.execute("show tables;"))
    #except:
        #print("ERROR!")

def dist_time_finder(place1,place2):
        #finds distance and time between two places
        matrix = gmaps.distance_matrix(place1,place2)['rows'][0]['elements'][0]
        distance = matrix['distance']['text']
        time = matrix['duration']['text']
        return (distance,time)
def drop_tables():
        #drops all the tables
        for i in table_names:
                execute_str = "drop table " + i + ";"
                cur.execute(execute_str)
                
def insert_value(table_name,values):
    #values is converted to a string of all values, comma seperated, to be inserted
    #try:
        if mycon.is_connected():
            val_str = ""
            for i in values:
                val_str = val_str + i +","
            val_str = val_str[:-1]
            #print(val_str)
            execute_str = "insert into "+table_name+" values ("+val_str+");"
            #print(execute_str)
            cur.execute(execute_str)
            mycon.commit()
            
   #except:
        #print("ERROR!")


def location(place):
    #gives a tuple of the latitude and longitude from an address
    geocode_result = gmaps.geocode(place)
    
    lat=geocode_result[0]['geometry']["location"]['lat']
    lng=geocode_result[0]['geometry']["location"]['lng']
    location=(lat,lng)
    return location

def address(lat,lon):
        #gives an address from the latitude and longitude
        f = (lat,lon)
        address_list = gmaps.reverse_geocode(f)
        address = address_list[0]['formatted_address']
        return address

def restaurantfinder(buy_name):
    #searches for restaurants in a 5km radius
    execute_str = 'select * from Buyers where username="' + buy_name + '";'
    cur.execute(execute_str)
    all = cur.fetchone()
    Buyer_lat = all[2]
    Buyer_long = all[3]
    conversion_factor_lat = 1 / 111 * 5
    conversion_factor_lng = 5 / (math.sin(math.degrees(1)) * Buyer_lat)
    maxlat = Buyer_lat + conversion_factor_lat
    minlat = Buyer_lat - conversion_factor_lat
    maxlong = Buyer_long + conversion_factor_lng
    minlong = Buyer_long - conversion_factor_lng
    exec2 = 'select * from Sellers where location_lat between ' + str(minlat) + ' and ' + str(maxlat) + ';'
    cur.execute(exec2)
    out1 = cur.fetchall()
    exec3 = 'select * from Sellers where location_lon between ' + str(minlong) + ' and ' + str(maxlong) + ';'
    cur.execute(exec3)
    out2 = cur.fetchall()
    out = []
    for i in out1:
        for j in out2:
            if i == j:
                out = out + [i]
    return out

def delivery_dude_finder(restaurant):
    #finds nearby delivery people in a 5km radius
    execute_str = 'select * from Sellers where username="' + restaurant + '";'
    cur.execute(execute_str)
    all = cur.fetchone()
    Seller_lat = all[2]
    Seller_long = all[3]
    conversion_factor_lat = 1 / 111 * 5
    conversion_factor_lng = 5 / (math.sin(math.degrees(1)) * Seller_lat)
    maxlat = Seller_lat + conversion_factor_lat
    minlat = Seller_lat - conversion_factor_lat
    maxlong = Seller_long + conversion_factor_lng
    minlong = Seller_long - conversion_factor_lng
    exec2 = 'select * from Delivery_Dude where location_lat between ' + str(minlat) + ' and ' + str(maxlat) + ';'
    cur.execute(exec2)
    out1 = cur.fetchall()
    exec3 = 'select * from Delivery_Dude where location_lon between ' + str(minlong) + ' and ' + str(maxlong) + ';'
    cur.execute(exec3)
    out2 = cur.fetchall()
    out = []
    for i in out1:
        for j in out2:
            if i == j:
                out = out + [i]
                break
                
    return out


def login(usernm, pwd, table):
    #allows users to log in
    execute_str = 'select * from ' + table + ' where username="' + usernm + '";'
    cur.execute(execute_str)
    out = cur.fetchone()
    if out != None:
        pwd_real = out[1]
        if pwd_real == pwd:
            print("Log in Complete")

            # go to main page
            if table == "Buyers":
                buyer_main_page(usernm)
            elif table == "Sellers":
                seller_main_page(usernm)
            elif table == "Delivery_Dude":
                delivery_dude_main_page(usernm)
        
        else:
            print("The password you have entered is wrong, please try again")
    else:
        print("Invalid username")

def signup(name,table,password,address):
        #allows admin to create new accounts
        loc1_latitude,loc1_longitude=location(address)
        if table == "Delivery_Dude":
                insert_value(table,['"'+name+'"','"'+password+'"',str(loc1_latitude),str(loc1_longitude)])
        else:
                insert_value(table,['"'+name+'"','"'+password+'"',str(loc1_latitude),str(loc1_longitude),'"'+address+'"'])
        
def buyer_main_page(username):
        #the main page for the buyers
        nearby_restaurants= restaurantfinder(username)
        orders_placed = dict()
        #print(nearby_restaurants)
        for i in nearby_restaurants:
                temp_name = str(i[0])
                #execute_str = 'select * from Menu where seller_name="' + temp_name + '";'
                #cur.execute(execute_str)
                #temp_menu = cur.fetchall()
                print("Restaurant Name: ", temp_name)
                #for i in temp_menu:
                    #print(i)
        total_price = 0
        order_str = ""
        print("ORDER PLACEMENT")
        restaurant_names=[]
        while True:
                restaurant = input("Enter restaurant name: ")
                for i in nearby_restaurants:
                        restaurant_names.append(i[0])
                if restaurant in restaurant_names:
                        break
                else:
                        print("Enter valid restaurant name")
        
        orders =list()
        while True:
            update()
            #menu for buyers
            print("BUYERS MAIN PAGE")
            print("1.Place Order")
            print("2.Finish Order")
            print("3.View Time")
            print("4.Print Menu")
            x = input("Enter choice: ")
            if x=="1":
                #placing orders
                food_type = input("Enter food name: ")
                food_quantity = int(input("Enter amount of food: "))
                orders_placed[food_type] = food_quantity
                execute_str = 'select * from Menu where seller_name="' + restaurant + '" and food_type="' + food_type + '";'
                cur.execute(execute_str)
                menu = cur.fetchone()
                temp_price = int(menu[3])
                total_price = total_price + (temp_price*food_quantity)
                
                order_str= order_str + str(food_quantity) + " " + food_type + " "
                print(order_str)
                #orders.append(order_str)
            elif x=="2":
                print("Finishing up your order for:", order_str)
                break
            elif x=="3":
                    execute_str = 'select address from buyers where username="' + username + '";'
                    cur.execute(execute_str)
                    buyer_address = cur.fetchone()[0]
                    execute_str1 = 'select address from sellers where username="' + restaurant + '";'
                    cur.execute(execute_str1)
                    seller_address = cur.fetchone()[0]
                    distance,time = dist_time_finder(buyer_address,seller_address)
                    print("Distance to restaurant: ",distance)
                    print("Expected time from restaurant: ",time)
            elif x=="4":
                    execute_str_7 = 'select * from Menu where seller_name="' + restaurant + '";'
                    cur.execute(execute_str_7)
                    temp_menu = cur.fetchall()
                    if temp_menu != []:
                        print("MENU:")
                        df = pandas.DataFrame(temp_menu, columns = ['Seller Name','Food Type','Food Quantity','Food Price'])
                        print(df)
                    else:
                        print("Menu Empty")
            else:
                print("Invalid input")
        possible_delivery = delivery_dude_finder(restaurant)
        delivery_dude = None
        #print(possible_delivery)
        for i in possible_delivery:
            execute_str_2 = 'select * from Orders where delivery_dude_name="' + i[0]+ '";'
            cur.execute(execute_str_2)
            status_dd = cur.fetchone()
            #execute_str_3 = 'select * from Orders where buyer_name= "' + username + '" and seller_name = "' + restaurant + '" and status = 0;'
            #cur.execute(execute_str_3)
            if status_dd == None:
                delivery_dude = i

        if delivery_dude != None:
                delivery_dude_name = delivery_dude[0]
                exec_2 = 'insert into Orders values(0,"' + username + '","' + restaurant + '","' + delivery_dude_name + '","' + order_str + '","' + str(total_price) + '");'
                cur.execute(exec_2)
                for k in orders_placed:
                        execute_str_5 = 'update menu set food_quantity = food_quantity - ' + str(orders_placed[k]) + ' where food_type = "' + k + '";'
                        cur.execute(execute_str_5)
                        mycon.commit()
        else:
            print("Your order could not be placed")
            print("Can't find a delivery dude")

def seller_main_page(username):
    while True:
        update()
        print("SELLERS MAIN PAGE")
        print("1.View Orders")
        print("2.Add to Menu")
        print("3.Delete from Menu")
        print("4.Update Menu")
        print("5. Print Current Menu")
        print("6.Quit")
        o = input("Enter choice: ")
        if o == '1':
            execute_str = 'select * from Orders where seller_name="' + username + '";'
            #print(execute_str)
            cur.execute(execute_str)
            order = cur.fetchall()
            if order != []:
                  for i in range(len(order)):
                      print("Order Number: ",i+1)
                      print("Buyer Name: ", order[i][1])
                      print("Delivery Person Name: ",order[i][3])
                      print("Order Details: ",order[i][4])
                      print("Order Amount: ", order[i][5])
            else:
                  print("Order Empty")
        elif o == '2':
            food = input("Enter food item name: ")
            price = int(input("Enter food price per unit: "))
            # typ=input("enter food type")
            quantity = int(input("Enter quantity left: "))
            exec_str = "insert into Menu values('" + username + "','" + food + "','" + str(quantity) + "','" + str(
                price) + "');"
            #print(exec)
            cur.execute(exec_str)
            mycon.commit()
        elif o == '3':
            food = input("Enter food item name to be deleted: ")
            cur.execute(f"delete from menu where food_type='{food}';")
            mycon.commit()
        elif o=='4':
               food = input("Enter food item name to be Updated: ")
               qty=int(input("Enter new quantity: "))
               cur.execute(f"update menu set food_quantity='{qty}' where food_type='{food}';")
               mycon.commit()
        elif o == '5':
                    execute_str_7 = 'select * from Menu where seller_name="' + username + '";'
                    cur.execute(execute_str_7)
                    temp_menu = cur.fetchall()
                    if temp_menu != []:
                        print("MENU:")
                        df = pandas.DataFrame(temp_menu, columns = ['Seller Name','Food Type','Food Quantity','Food Price'])
                        print(df)
                    else:
                        print("Menu Empty")
        elif o=='6':
            break
        else:
            print("Invalid Input")


def delivery_dude_main_page(username):
        #mainpage for the delivery persons
        while True:
                update()
                print("DELIVERY DUDE MAIN PAGE")
                print("1. View Order")
                print("2.Order Details")
                print("3.Update Status")
                print("4.Quit")
                o = input("Enter choice: ")
                if o == '1':
                        execute_str = 'select orders.buyer_name, buyers.address  as "buyer_address" ,orders.seller_name, sellers.address as "seller_address" from buyers,sellers,orders where status!=2 and orders.buyer_name=buyers.username and sellers.username=orders.seller_name and orders.delivery_dude_name="'+username+'";'
                        cur.execute(execute_str)
                        order = cur.fetchall()
                        if order != []:
                            print("Buyer Name: ", order[0][0])
                            print("Buyer Address: ",order[0][1])
                            print("Seller Name: ",order[0][2])
                            print("Seller Address: ", order[0][3])
                        else:
                            print("Order Empty")
                elif o == '2':
                    execute_str = 'select status from orders where orders.delivery_dude_name="' + username + '";'
                    cur.execute(execute_str)
                    status1 = cur.fetchall()
                    status = status1[0][0]
                    if status == 1:
                        execute_str = 'select  buyers.address  as "buyer_address" , sellers.address as "seller_address" from buyers,sellers,orders where orders.buyer_name=buyers.username and sellers.username=orders.seller_name and orders.delivery_dude_name="' + username + '";'
                        cur.execute(execute_str)
                        order = cur.fetchall()
                        place1 = order[0][0]
                        place2 = order[0][1]
                        distance,time = dist_time_finder(place1,place2)
                        print("Distance from seller to buyer: ",distance)
                        print("Time from seller to buyer: ",time)
                    elif status == 0:
                        execute_str = 'select  delivery_dude.location_lat, delivery_dude.location_lon , sellers.address as "seller_address" from delivery_dude,sellers,orders where orders.delivery_dude_name=delivery_dude.username and sellers.username=orders.seller_name and orders.delivery_dude_name="' + username + '";'
                        cur.execute(execute_str)
                        order = cur.fetchall()
                        place1 = address(order[0][0],order[0][1])
                        place2 = order[0][2]
                        distance,time = dist_time_finder(place1,place2)
                        print("Distance from me to seller: ",distance)
                        print("Time from me to seller: ",time)
                elif o == '3':
                        print('Enter status as 2 if delivered and 1 if picked up')
                        status_new = input("Enter status: ")
                        execute_str = 'update orders set status ='+status_new+" where delivery_dude_name ='" + username + "';"
                        if status_new == '2':
                                execute_str_2 = 'select buyers.location_lat, buyers.location_lon from buyers,orders where orders.delivery_dude_name = "' + username + '" and buyers.username = orders.buyer_name;'
                                cur.execute(execute_str_2)
                                new_lat, new_lon = cur.fetchall()[0]
                                execute_str_1 = 'update delivery_dude set location_lat= ' + str(new_lat) + ', location_lon= ' + str(new_lon) + ' where username = "' + username + '";'
                                cur.execute(execute_str_1)
                        elif status_new == '1':
                                execute_str_2 = 'select sellers.location_lat, sellers.location_lon from sellers,orders where orders.delivery_dude_name = "' + username + '" and sellers.username = orders.seller_name;'
                                cur.execute(execute_str_2)
                                new_lat, new_lon = cur.fetchall()[0]
                                execute_str_1 = 'update delivery_dude set location_lat= ' + str(new_lat) + ', location_lon= ' + str(new_lon) + 'where username = "' + username + '";'
                                cur.execute(execute_str_1)
                        cur.execute(execute_str)
                        mycon.commit()
                        print("Status Updated!!!!!!!!!!!!!")
                elif o == '4':
                        break
                else:
                        print("Wrong input")

def update():
        #removes extra data
        execute_str_1 = 'delete from menu where food_quantity = 0;'
        cur.execute(execute_str_1)
        mycon.commit()
        execute_str_2 = 'delete from orders where status = 2;'
        cur.execute(execute_str_2)
        mycon.commit()
        


#main 
while True:
        mycon.commit()
        update()
        print("Choose an Option (HOME PAGE)")
        print("1.Login")
        print("2.Sign Up")
        print("3.Quit")
        choice = input("Enter choice: ")

        if choice == "1":
                username = input("Enter username: ")
                password = input("Enter password: ")
                print("Select account type:")
                print("1. Buyers")
                print("2. Sellers")
                print("3. Delivery persons")
                x=input("Enter choice: ")
                table=None
                if x=="1":
                        table="Buyers"
                elif x=="2":
                        table="Sellers"
                elif x=="3":
                        table="Delivery_Dude"
                else:
                        print("Invalid Input")
                        
                if table!=None:
                        login(username,password,table)

        elif choice == "2":
                name=input("Enter name: ")
                password=input("Enter password: ")
                address=input("Enter address: ")
                print("Select account type:")
                print("1. Buyers")
                print("2. Sellers")
                print("3. Delivery persons")
                x=input("Enter choice: ")
                table=None
                if x=="1":
                        table="Buyers"
                elif x=="2":
                        table="Sellers"
                elif x=="3":
                        table="Delivery_Dude"
                else:
                        print("Invalid Input")
                if table!=None:
                        signup(name,table,password,address)

        elif choice == "3":
                break

        else:
                print("Invalid Input")
