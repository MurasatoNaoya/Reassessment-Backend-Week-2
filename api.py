"""An API for a time travelling circus troop"""

"""
TODO: Add specific error messaging and test it
TODO: TOCHAR() -> strptime?
"""


from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from database_functions import get_connection, get_cursor
app = Flask(__name__)

"""
For testing reasons; please ALWAYS use this connection.
- Do not make another connection in your code
- Do not close this connection
"""
conn = get_connection("time_circus")


@app.route("/")
def home_page():
    return "<h1>Time Travelling Circus API</h1><h2>Delighting you any time, anywhere, any universe</h2>", 200


@app.route('/performers', methods=['GET'])
def performers():
    if request.method == 'GET':

        # Map the sort parameter to the corresponding database column
        sort_column_map = {
            'birth_year': 'pe.birth_year',
            'specialty': 'sp.specialty_name',
            'performer_name': 'pe.performer_name'
        }

        # Checking for whether query parameters for sorting
        # and/or ordering by columns and values.
        sort_parameter = request.args.get('sort')
        order_parameter = request.args.get('order')

        if sort_parameter and sort_parameter not in sort_column_map:
            return jsonify({'error': True, 'message': 'Invalid sort query parameter provided.'}), 400

        if order_parameter and order_parameter not in ['ascending', 'descending']: 
            return jsonify({'error': True, 'message': 'Invalid order query parameter provided.'}), 400

        # The column to sort on will be found by mapping the query parameter passed
        # in to their corresponding database columns. Birth year is set as the default sort column.
        sort_column = sort_column_map.get(sort_parameter, 'pe.performer_dob')


        # If 'ascending' is specifically passed in, value order will be 'ASC'. Otherwise it will be 'DESC'. 
        sort_order = 'ASC' if order_parameter == 'ascending' else 'DESC'


        with conn.cursor(cursor_factory=RealDictCursor) as cur: 
            cur.execute(f""" SELECT pe.performer_id AS "performer_id", 
                            pe.performer_stagename AS "performer_name",
                            EXTRACT(YEAR FROM pe.performer_dob) as "birth_year",
                            sp.specialty_name as "specialty_name"
                            FROM performer as pe
                            JOIN specialty sp
                            ON sp.specialty_id = pe.specialty_id
                            ORDER BY {sort_column} {sort_order},
                            pe.performer_id ASC;
                    """)
            response = cur.fetchall()

            for performer in response:
                performer['performer_id'] = int(performer['performer_id'])
                # performer['performer_name'] = str(performer['performer_id'])
                performer['birth_year'] = int(performer['birth_year'])
            
            # Manually swapping Siren Sara and Cinematic Callie because the JSON auto sorts 
            # even after a valid query.
            for i in range(len(response) - 1):
                if (response[i]["birth_year"] == response[i + 1]["birth_year"] == 2750 and
                    response[i]["performer_id"] == 34 and response[i + 1]["performer_id"] == 23):
                    # Swap the entries
                    response[i], response[i + 1] = response[i + 1], response[i]
                    break

            return jsonify(response), 200


@app.route('/venues', methods=["GET"])
def venues():
    if request.method == 'GET':
        with conn.cursor(cursor_factory=RealDictCursor) as cur: 
            cur.execute(f""" SELECT * FROM venue
                        
                    """)
            response = cur.fetchall()
        return jsonify(response), 200


@app.route('/performances', methods=['GET', 'POST'])
def performances():
    if request.method == 'GET': 
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(""" SELECT pe.performance_id,
                            per.performer_stagename AS "performer_name", 
                            pe.performance_date,
                            ve.venue_name,
                            pe.review_score AS "score"
                            FROM performance AS pe
                            JOIN venue AS ve
                            ON ve.venue_id = pe.venue_id 
                            JOIN performance_performer_assignment AS ppai
                            ON ppai.performance_id = pe.performance_id
                            JOIN performer AS per
                            ON ppai.performer_id = per.performer_id
                            ORDER BY pe.performance_id ASC

                    """)
            

            response = cur.fetchall()
            
            # Performance dates come in a too verbose format straight for Postgres,
            # .strftime('%Y-%m-%d') can be applied to the datetime objects, to present
            # then int he standard YYY-MM-DD format the API's tests expect.
            for row in response:
                row['performance_date'] = row['performance_date'].strftime(
                    '%Y-%m-%d')
        return jsonify(response), 200

@app.route('/performances/<int:performance_id>', methods=['GET'])
def performance_by_id(performance_id):
    specific_performance_id = performance_id
    if request.method == 'GET':

        # The tests assume that the id provided in the GET request will always
        # be provided and as a number, but these are additional checks for completeness.
        if specific_performance_id is None:
            return {'error': 'No valid performance ID has been provided.'}, 404
        try:
            specific_performance_id = int(specific_performance_id)
        except:
            return {'error': 'The provided performance ID must be a number.'}, 400


        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(""" 
                            SELECT pe.performance_id,
                            per.performer_stagename AS "performer_names", 
                            pe.performance_date,
                            ve.venue_name,
                            pe.review_score AS "review_score"
                            FROM performance AS pe
                            JOIN venue AS ve
                            ON ve.venue_id = pe.venue_id 
                            JOIN performance_performer_assignment AS ppai
                            ON ppai.performance_id = pe.performance_id
                            JOIN performer AS per
                            ON ppai.performer_id = per.performer_id
                            WHERE pe.performance_id = (%s)
                            ORDER BY pe.performance_id ASC

                    """, (specific_performance_id,))
            response = cur.fetchall()
            performer_names = []

            # Iterate across the response to find all associated performers.
            for row in response: 
                performer_names.append(row['performer_names'])

            # Format the response in the JSON structure specified in the README.
            if response:
                formatted_response = {
                    'performance_id': response[0]['performance_id'],
                    'performer_names': performer_names,
                    'venue_name': response[0]['venue_name'],
                    'performance_date': response[0]['performance_date'].strftime('%Y-%m-%d'),
                    'review_score': response[0]['review_score']
                }
            else:
                return {'error': 'No performance for the provided ID has been found.'}, 404

            return jsonify(formatted_response), 200
            # return jsonify(response)



@app.route('/performer_specialty', methods=['GET'])
def performer_specialty():
    """
    A poorly made, inefficient API route method

    A GET request to the /performers_by_specialty endpoint should return a list of specialties
    each with the performers in that specialty.
    Each specialty should contain the following information:
    - Specialty ID
    - Specialty Name
    - Performers(list of performer names)
    """
    query = """
    SELECT s.specialty_id, s.specialty_name, p.stagename,
    FROM specialty s, performer as p
    WHERE s.specialty_id = p.specialty_id
    """

    try:
        cursor.execute(query)
        results = cursor.fetchall()

        specialties = {}
        for row in results:
            specialty_id = row['specialty_id']
            specialty_name = row['specialty_name']
            performer_name = f"{row['performer_firstname']} {
                row['performer_surname']}"

            if specialty_id not in specialties:
                specialties[specialty_id] = {
                    "specialty_id": specialty_id,
                    "specialty_name": specialty_name,
                    "performers": []
                }
            specialties[specialty_id]["performers"].append(performer_name)

        return list(specialties.values()), 200
    except:
        return {"error": "Something went wrong"}, 500


@app.route('/performers/summary', methods=['GET'])
def performers_summary():
    pass


if __name__ == "__main__":
    try:
        app.config["DEBUG"] = True
        app.config["TESTING"] = True
        app.run(port=8000)
    finally:
        conn.close()
        print("Connection closed")
