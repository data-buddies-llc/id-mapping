from addict import Dict
from db_utils import load_config, get_postgres_connection, get_retailer_by_key, RetailerDC, get_mongo_connection
from psycopg.rows import dict_row


config = load_config('etc/config.yml')
retailer :RetailerDC = get_retailer_by_key(2)

def load_mappings(retailer_key):
    sql = "select * from tjs_retailer_ids_mapping where retailer_key = %s"
    cur = retailer.postgres.cursor(row_factory=dict_row)
    cur.execute(sql, (retailer_key,))
    result = []
    for row in cur:
        result.append(dict(row.items()))
    return result


def get_mapped_values(mappings, doc):
    values = []
    for mapping in mappings:
        name = mapping['name']
        value = doc[mapping['path']]
        values.append((name, value))
    return values


def save_id_map(retailer_key, values, model_number, conn):
    sql = 'update "Products" set identifiers = identifiers || %(data)s :: hstore where "SourceKey" = %(key)s and "ModelNumber" = %(model)s '

    dval = dict(values)
    cur = conn.cursor()
    cur.execute(sql, ({'data': dval, 'key': retailer_key, 'model': model_number}))
    conn.commit()
    cur.close()


def map_ids():
    mappings = load_mappings(2)
    cursor = retailer.mongo_scrape.find({})
    conn = get_postgres_connection()
    i = 0
    for document in cursor:
        i += 1
        doc = Dict(document)
        model_number = doc[retailer.model_key]
        values = get_mapped_values(mappings, doc)

        save_id_map(2, values, model_number, conn)
        print(f'saving: {model_number}')

map_ids()
