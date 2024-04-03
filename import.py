import pandas as pd
import pymysql
import logging

# Logging configuration
logging.basicConfig(filename='import_data.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def read_data(file_path, file_type):
    if file_type == 'csv':
        return pd.read_csv(file_path)
    elif file_type == 'json':
        return pd.read_json(file_path)
    elif file_type == 'excel':
        return pd.read_excel(file_path)
    else:
        raise ValueError("Invalid file type")


def insert_data(cursor, table_name, data_frame, columns):
    for index, row in data_frame.iterrows():
        try:
            # Generate SQL query dynamically
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row[col] for col in columns))
            logging.info(f"Data inserted into {table_name}: {row}")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")


def import_data(host, user, password, database):
    # Read data from source files
    episodes_data = read_data('episodes.csv', 'csv')
    artists_data = read_data('artists.json', 'json')
    techniques_data = read_data('techniques.xlsx', 'excel')

    # Establish database connection
    try:
        connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     database=database)
        with connection.cursor() as cursor:
            # Insert episodes data
            insert_data(cursor, 'Episode', episodes_data, [
                        'title', 'season', 'episode_number', 'air_date', 'description'])
            # Insert artists data
            insert_data(cursor, 'Artist', artists_data, ['name'])
            # Insert techniques data
            insert_data(cursor, 'PaintingTechnique', techniques_data, ['name'])

        # Commit changes
        connection.commit()
        logging.info("Data successfully imported into the database.")
    except Exception as e:
        logging.error(f"Error connecting to database or inserting data: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


if __name__ == "__main__":
    import_data(host='localhost', user='your_username',
                password='your_password', database='joy_of_painting')
