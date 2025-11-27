import sqlite3
import time
from datetime import datetime
import json
import smbus2
import csv

# Define class for the sound meter
class SoundMeter:
    def __init__(self, address=0x48, bus_number=1):
        # initalize the i2c bus
        self.bus = smbus2.SMBus(bus_number)
        self.address = address

    def read_decibels(self):
        # write the register address (0x0A) and read one byte
        return self.bus.read_byte_data(self.address, 0x0A)

    def close(self):
        self.bus.close()

# Function to insert batch
# def insert_batch(batch_data):
#    """
#    batch_data: list of tuples [(timestamp1, value1), (timestamp2, value2), ...]
#    """
#    cursor.executemany(
#        'INSERT OR IGNORE INTO sensor_readings (timestamp, value) VALUES (?, ?)',
#        batch_data
#    )
#    conn.commit()
#    print(f"Inserted {cursor.rowcount} new records (duplicates ignored)")

# Example usage
# batch1 = [(1000, 23.5), (1001, 24.1), (1002, 23.8)]
# insert_batch(batch1)

# If you run the same batch again, it won't duplicate
# insert_batch(batch1)  # Will insert 0 records


# from queue import Queue
# import threading
#
# batch_queue = Queue()
#
# def process_batches():
#     while True:
#         batch = batch_queue.get()
#         insert_batch(batch)
#         batch_queue.task_done()
#
# # Start processing thread
# threading.Thread(target=process_batches, daemon=True).start()
#
# # When data arrives, add to queue
# batch_queue.put(new_batch)
# Create connection and table

if __name__ == '__main__':

    # initalize the database if it doesn't already exist
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()

    # Create table with unique constraint on timestamp
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
        timestamp INTEGER PRIMARY KEY,
        year INT NOT NULL,
        month INT NOT NULL, 
        day INT NOT NULL, 
        hour INT NOT NULL, 
        second INT NOT NULL,
        db_level REAL NOT NULL
    )
                   ''')
    conn.commit()

    # main loop 
    try: 
        # intialize the sound meter 
        sound_meter = SoundMeter()
        print("Sound meter intialized. Press ctrl + c to exit.")

        # initialize the batch list
        batch = []

        while True:
            try:
                # read the decibel value
                timestamp = int(time.time())

                # getthe time categories
                dt_object = datetime.fromtimestamp(current_timestamp)
                dt_year = dt_object.year
                dt_month = dt_object.month
                dt_day = dt_object.day
                dt_hour = dt_object.hour
                dt_sec = dt_object.second

                # read the decibel level
                db_level = read_decibels()
            
                if db_level > 65:
                    # append the reading
                    batch.append((timestamp, dt_year, dt_month, dt_day, dt_hour, dt_sec, db_level))
                    # pause
                    time.sleep(0.5)

                    if len(batch) >= 100: 
                        # Commit every 100 readings
                        cursor.executemany(
                                'INSERT OR IGNORE INTO sensor_readings (timestamp, year, month, day, hour, second, db_level) VALUES (?, ?, ?, ?, ?, ?, ?)',
                                batch
                                )
                        conn.commit()
                        # blank out the batch
                        batch = []

            except IOError as e:
                print(f"I2C Error: {e}")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nProgram terminated by user")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sound_meter.close()

