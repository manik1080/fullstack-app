from flask import Flask, request, jsonify
import mysql.connector
from config import Passwords

app = Flask(__name__)

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': Passwords.mysql,
    'database': 'ecommerce',
    'autocommit': True
}

def get_db_connection():
    return mysql.connector.connect(**db_config)

# Utility to fetch columns for a table
TABLE_COLUMNS = {
    'distribution_centers': ['id', 'name', 'latitude', 'longitude'],
    'inventory_items': ['id', 'product_id', 'created_at', 'sold_at', 'cost', 'product_category',
                        'product_name', 'product_brand', 'product_retail_price', 'product_department',
                        'product_sku', 'product_distribution_center_id'],
    'orders': ['order_id', 'user_id', 'status', 'gender', 'created_at', 'returned_at',
               'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'inventory_item_id', 'status',
                    'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'cost', 'category', 'name', 'brand', 'retail_price', 'department',
                 'sku', 'distribution_center_id'],
    'users': ['id', 'first_name', 'last_name', 'email', 'age', 'gender', 'state',
              'street_address', 'postal_code', 'city', 'country', 'latitude', 'longitude',
              'traffic_source', 'created_at']
}

# Generic CRUD operations
@app.route('/<table>', methods=['GET', 'POST'])
def handle_collection(table):
    if table not in TABLE_COLUMNS:
        return jsonify({'error': 'Unknown table'}), 404

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if request.method == 'GET':
        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return jsonify(rows)
        except Exception as e:
            cursor.close()
            conn.close()
            return jsonify({'error': str(e)}), 500

    if request.method == 'POST':
        try:
            data = request.get_json()
            cols = TABLE_COLUMNS[table]
            fields = ', '.join(cols[1:])  # exclude PK for insert
            placeholders = ', '.join(['%s'] * (len(cols) - 1))
            values = [data.get(c) for c in cols[1:]]
            sql = f"INSERT INTO {table} ({fields}) VALUES ({placeholders})"
            cursor.execute(sql, values)
            conn.commit()
            new_id = cursor.lastrowid
            
            # Fetch the created record to return it
            cursor.execute(f"SELECT * FROM {table} WHERE {cols[0]} = %s", (new_id,))
            created_record = cursor.fetchone()
            cursor.close()
            conn.close()
            return jsonify(created_record), 201
        except Exception as e:
            cursor.close()
            conn.close()
            return jsonify({'error': str(e)}), 500

@app.route('/<table>/<int:record_id>', methods=['GET', 'PUT', 'DELETE'])
def handle_item(table, record_id):
    if table not in TABLE_COLUMNS:
        return jsonify({'error': 'Unknown table'}), 404

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    pk = TABLE_COLUMNS[table][0]

    if request.method == 'GET':
        try:
            cursor.execute(f"SELECT * FROM {table} WHERE {pk} = %s", (record_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            if row:
                return jsonify(row)
            return jsonify({'error': 'Not found'}), 404
        except Exception as e:
            cursor.close()
            conn.close()
            return jsonify({'error': str(e)}), 500

    if request.method == 'PUT':
        data = request.get_json()
        cols = TABLE_COLUMNS[table][1:]
        set_clause = ', '.join([f"{col} = %s" for col in cols])
        values = [data.get(col) for col in cols]
        values.append(record_id)
        sql = f"UPDATE {table} SET {set_clause} WHERE {pk} = %s"
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Updated'})

    if request.method == 'DELETE':
        cursor.execute(f"DELETE FROM {table} WHERE {pk} = %s", (record_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Deleted'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
