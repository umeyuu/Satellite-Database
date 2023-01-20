from dotenv import load_dotenv
load_dotenv()
import os
import MySQLdb



connection = MySQLdb.connect(
  host= os.getenv("PLANETSCALE_HOST"),
  user=os.getenv("USERNAME"),
  passwd= os.getenv("PASSWORD"),
  db= os.getenv("DATABASE"),
  ssl_mode = "VERIFY_IDENTITY",
  ssl = {
    "ca": "/etc/ssl/cert.pem"
  }
)

# Create cursor and use it to execute SQL command
cursor = connection.cursor()
cursor.execute("select @@version")
version = cursor.fetchone()

if version:
    print('Running version: ', version)
else:
    print('Not connected.')