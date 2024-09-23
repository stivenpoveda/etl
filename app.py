from flask import Flask, render_template, request
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import matplotlib
import spacy

matplotlib.use('Agg')

app = Flask(__name__)


nlp = spacy.load("es_core_news_sm")


db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = '5432'
db_name = 'chinook'


engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def transform_data():
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
    df['total_price'] = df['unit_price'] * df['quantity']
    df['numerador'] = range(1, len(df) + 1)
    df.to_sql('etl_large_data', con=engine, if_exists='replace', index=False)
    print("Datos cargados en la base de datos con éxito")

transform_data()

def get_data():
    query = "SELECT * FROM etl_large_data"
    df = pd.read_sql(query, con=engine)
    return df

def analyze_question(question, df):
    doc = nlp(question)
    
    if 'total' in question.lower() and 'precio' in question.lower():
        result = df.groupby('city')['total_price'].sum()
        return result.to_frame().reset_index().to_html(classes='table table-striped', index=False)
    elif 'clientes' in question.lower() and 'ciudad' in question.lower():
        result = df['city'].value_counts()
        return result.to_frame().reset_index().rename(columns={'index': 'Ciudad', 'city': 'Número de Clientes'}).to_html(classes='table table-striped', index=False)
    else:
        return "No puedo responder a esa pregunta."

@app.route('/', methods=['GET', 'POST'])
def index():
    df = get_data()

 
    if 'city' in df.columns:
        genre_counts = df['city'].value_counts()
        top_cities = genre_counts.head(10)
        img_pie = io.BytesIO()
        plt.figure()
        top_cities.plot(kind='pie', autopct='%1.1f%%', figsize=(8, 8), colors=plt.get_cmap('tab10').colors)
        plt.title('Distribución de Ciudades')
        plt.tight_layout()
        plt.savefig(img_pie, format='png')
        img_pie.seek(0)
        plot_url_pie = base64.b64encode(img_pie.getvalue()).decode()
        plt.clf()
    else:
        plot_url_pie = None

    if 'city' in df.columns and 'total_price' in df.columns:
        city_totals = df.groupby('city')['total_price'].sum().sort_values(ascending=False)
        top_city_totals = city_totals.head(10)
        img_bar = io.BytesIO()
        plt.figure()
        top_city_totals.plot(kind='bar', figsize=(10, 6), color=plt.get_cmap('tab10').colors)
        plt.title('Total de Precio por Ciudad')
        plt.ylabel('Total Precio')
        plt.xlabel('Ciudad')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(img_bar, format='png')
        img_bar.seek(0)
        plot_url_bar = base64.b64encode(img_bar.getvalue()).decode()
        plt.clf()
    else:
        plot_url_bar = None

    img_scatter = io.BytesIO()
    plt.figure()
    scatter_plot = df.plot(kind='scatter', x='quantity', y='total_price', c='total_price', cmap='viridis', alpha=0.5, figsize=(10, 6))
    plt.title('Cantidad vs Precio Total')
    plt.ylabel('Total Precio')
    plt.xlabel('Cantidad')
    plt.colorbar(scatter_plot.get_children()[0], label='Total Precio')
    plt.tight_layout()
    plt.savefig(img_scatter, format='png')
    img_scatter.seek(0)
    plot_url_scatter = base64.b64encode(img_scatter.getvalue()).decode()
    plt.clf()

    answer = None
    if request.method == 'POST':
        question = request.form.get('question')
        if question:
            answer = analyze_question(question, df)

    return render_template('index.html', plot_img_pie=plot_url_pie, plot_img_bar=plot_url_bar, plot_img_scatter=plot_url_scatter, answer=answer)

if __name__ == '__main__':
    app.run(debug=True)
