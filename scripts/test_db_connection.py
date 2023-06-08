import psycopg2

try:
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host='localhost',
        port='5432',
        dbname='canpl_dev',
        user='duriandaddy',
        password='beanpole'
    )

    # Print a success message if the connection is successful
    print("Connection to PostgreSQL database successful!")

    # Close the connection
    connection.close()

except (psycopg2.Error) as error:
    # Print the error message if the connection fails
    print("Error while connecting to PostgreSQL database:", error)
