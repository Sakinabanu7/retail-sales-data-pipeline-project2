import apache_beam as beam
import psycopg2
from apache_beam.options.pipeline_options import PipelineOptions

# PostgreSQL database connection config
db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',  # you confirmed this
    'host': 'localhost',
    'port': '5432'
}

class InsertToPostgres(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config

    def setup(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def process(self, element):
        try:
            values = element.strip().split(",")
            query = """
                INSERT INTO sales_cleaned (
                    SaleID, CustomerID, CustomerName, Email, Phone,
                    ProductID, ProductName, Category, SaleDate, Quantity, TotalAmount
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.execute(query, values)
            self.conn.commit()
        except Exception as e:
            print("Insert failed:", e)

    def teardown(self):
        self.cursor.close()
        self.conn.close()

def run():
    options = PipelineOptions(runner='DirectRunner')

    with beam.Pipeline(options=options) as p:
        (
            p
             
            | 'Read CSV Lines' >> beam.io.ReadFromText('final_cleaned_sales.csv', skip_header_lines=1)
            | 'Insert Into PostgreSQL' >> beam.ParDo(InsertToPostgres(db_config))
        )

    print("✅ Beam pipeline completed and data loaded into PostgreSQL.")

if __name__ == '__main__':
    run()

