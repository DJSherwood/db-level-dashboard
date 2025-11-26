import sqlite3
import time

# Create connection and table
conn = sqlite3.connect('sensor_data.db')
cursor = conn.cursor()

# Create table with unique constraint on timestamp
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_readings (
        timestamp INTEGER PRIMARY KEY,
        value REAL NOT NULL
    )
''')
conn.commit()

# Function to insert batch
def insert_batch(batch_data):
    """
    batch_data: list of tuples [(timestamp1, value1), (timestamp2, value2), ...]
    """
    cursor.executemany(
        'INSERT OR IGNORE INTO sensor_readings (timestamp, value) VALUES (?, ?)',
        batch_data
    )
    conn.commit()
    print(f"Inserted {cursor.rowcount} new records (duplicates ignored)")

# Example usage
batch1 = [(1000, 23.5), (1001, 24.1), (1002, 23.8)]
insert_batch(batch1)

# If you run the same batch again, it won't duplicate
insert_batch(batch1)  # Will insert 0 records



batch = []
while True:
    timestamp = int(time.time() * 1000)
    value = read_decibel_sensor()
    batch.append((timestamp, value))

    if len(batch) >= 100:  # Commit every 100 readings
        cursor.executemany(
            'INSERT OR IGNORE INTO sensor_readings (timestamp, value) VALUES (?, ?)',
            batch
        )
        conn.commit()
        batch = []

    time.sleep(0.01)

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