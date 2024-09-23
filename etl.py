from sqlalchemy import create_engine
import pandas as pd

  
db_user = 'postgres'       
db_password = '12345'     
db_host = 'localhost'     
db_port = '5432'           
db_name = 'chinook'      


engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


query = """
SELECT invoice_line.invoice_id, invoice_line.unit_price, invoice_line.quantity, album.title, 
       customer.customer_id, customer.first_name, customer.last_name, customer.city, 
       track.name as track_name
FROM invoice_line
JOIN track ON invoice_line.track_id = track.track_id
JOIN album ON track.album_id = album.album_id
JOIN invoice ON invoice_line.invoice_id = invoice.invoice_id
JOIN customer ON invoice.customer_id = customer.customer_id
"""


df = pd.read_sql(query, con=engine)


print(f"Número de registros: {len(df)}")
print(df.head())


df['total_price'] = df['unit_price'] * df['quantity']


df.to_sql('etl_large_data', con=engine, if_exists='replace', index=False)

print("Datos cargados en la base de datos con éxito")
