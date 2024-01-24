import os
from datetime import datetime
from scr.extract import extract_transactional_data
from scr.transform import identify_and_remove_duplicates
from scr.load_data_to_s3 import df_to_s3
from dotenv import load_dotenv
load_dotenv()



dbname=os.getenv("dbname")
host=os.getenv("host")
port=os.getenv("port")
user=os.getenv("user")
password=os.getenv("password")


start_time=datetime.now()
print ("\n---Extracting the data from redshift---")
ot_transformed = extract_transactional_data(dbname, host, port, user, password)

print ("\n---Identifying and removing duplicates---")
ot_wout_duplicates = identify_and_remove_duplicates(ot_transformed)

key="transformations_final/nn_online_trans_transformed.pkl"
s3_bucket="sep-bootcamp"
aws_access_key_id=os.getenv("aws_access_key_id")
aws_secret_access_key=os.getenv("aws_secret_access_key_id")
print ("\n---Loading to S3---")
df_to_s3(ot_wout_duplicates, key, s3_bucket, aws_access_key_id, aws_secret_access_key)

execution_time= datetime.now() - start_time
print ("Execution time is:", execution_time)
