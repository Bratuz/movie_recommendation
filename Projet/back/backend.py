from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict
from itertools import combinations
import requests
import os


# Configuration BigQuery
project_id = "imperial-ally-417007"
dataset = "assignments2"
movies_table = "imperial-ally-417007.assignments2.movies"
ratings_table = "imperial-ally-417007.assignments2.rates"
links_table = "imperial-ally-417007.assignments2.links"
model_table = "imperial-ally-417007.assignments2.MF-model"
jsonkey_file_path = "/Users/kevin/Desktop/bonjour.json"
api_key = '207912d5baefe0c44b96b8f6a8110005'
credentials = service_account.Credentials.from_service_account_file(jsonkey_file_path)
client_query = bigquery.Client(credentials=credentials)

# Configuration Elasticsearch
URL_ENDPOINT = "https://ff4d964d84454a02adacd2be7108352a.europe-west9.gcp.elastic-cloud.com:443"
API_KEY = "amJUZ3BJNEJ3NHRUb3lpSlpTTFU6Z2E1SlloM1FSX3FMZWRsOWpGMXlkUQ"
INDEX_NAME = 'movies'
client = Elasticsearch(
    URL_ENDPOINT,
    api_key=API_KEY
)

# Dictionary to store user favorites
favorites = {}

# Create a Flask application
app = Flask(__name__)

# Route for the home page
@app.route("/")
def home():
    return "Home"

# Route for movie title autocomplete
@app.route("/autocomplete")
def autocomplete():
    # Get the search term from the request
    query = request.args.get('query')

    # Check if the search term is empty
    if not query:
        return jsonify([])  # Return an empty list if the search term is empty

    # Perform an Elasticsearch search to get autocomplete suggestions
    body = {
        "query": {
            "match_phrase_prefix": {
                "column2": {  # Search in the key 'column2' for movie titles
                    "query": query,
                    "max_expansions": 10  # Adjust the number of expansions as needed
                }
            }
        }
    }

    response = client.search(index=INDEX_NAME, body=body)
    suggestions = [hit['_source']['column2'] for hit in response['hits']['hits']]  # Retrieve movie titles
    return jsonify(suggestions)

# Route to add a movie to favorites
@app.route("/add_favorite", methods=["POST"])
def add_favorite():
    data = request.get_json()
    user_id = data.get("user_id")
    movie_id = data.get("movie_id")
    
    # Check if the user exists in the dictionary
    if user_id in favorites:
        # Add the movie to the existing user's favorites list
        favorites[user_id].append(movie_id)
    else:
        # Create a new entry for the user and add the movie
        favorites[user_id] = [movie_id]
    
    return jsonify({"message": "Movie added to favorites successfully"})

# Route to get the favorites list
@app.route("/get_favorites", methods=["GET"])
def get_favorites():
    user_id = request.args.get("user_id")
    if user_id in favorites:
        return jsonify(favorites[user_id])
    else:
        return jsonify([])  # Return an empty list if the user has no favorites

def user_rate():
    rate_user = {}
    try:
        query = f"""
            SELECT userId, ARRAY_AGG(movieId) AS rated_movies
            FROM `{ratings_table}`
            WHERE rating_im > 0.75
            GROUP BY userId
        """
        query_job = client_query.query(query)
        for row in query_job:
            user_id = row['userId']
            rated_movies = row['rated_movies']
            rate_user[user_id].append(rated_movies)
    except Exception as e:
        print(f"Failed to fetch or process data: {str(e)}")
    return rate_user

# Function to find similar users
def get_similar_users(target_user_id, ratings_data):
    # Dictionary to store movie ratings by user
    user_ratings = defaultdict(set)
    
    # Populate the dictionary with movie ratings by user
    for user_id, movie_id in ratings_data:
        user_ratings[user_id].add(movie_id)
    
    # Set of movies rated by the target user
    target_user_movies = user_ratings[target_user_id]
    
    # Calculate Jaccard similarity between the target user and each other user
    similarities = {}
    for user_id, movies in user_ratings.items():
        # Ignore the target user itself
        if user_id == target_user_id:
            continue
        
        # Calculate the intersection and union of movie sets
        intersection = len(target_user_movies.intersection(movies))
        union = len(target_user_movies.union(movies))
        
        # Calculate Jaccard similarity
        similarity = intersection / union
        
        similarities[user_id] = similarity
    
    # Sort users by similarity in descending order
    sorted_users = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    
    # Select the top 3 similar users
    top = [{"user_id": user_id, "similarity": similarity} for user_id, similarity in sorted_users[:3]]
    
    return top


def get_recommendations(user_id):
    recommendations = []
    similar_users = get_similar_users()

    # Ensure the IDs are integers rather than strings
    subquery = f"""
        SELECT userid
        FROM `{ratings_table}`
        WHERE userid IN ({', '.join(str(user_id) for user_id in similar_users)})
    """

    query = f"""
        SELECT DISTINCT predicted_rating_im_confidence, userId, movieId
        FROM ML.RECOMMEND(MODEL `{model_table}`, (SELECT userid FROM ({subquery})))
        WHERE predicted_rating_im_confidence > 0.75
        LIMIT 10
    """
    # Execute the query
    query_job = client_query.query(query)
    
    return recommendations

#List of the recommended movie titles
@app.route('/display_recommendations')
def display_recommendations():
    movie_data = []
    # Pass the user ID to the recommendation function to get recommendations
    recommendations = get_recommendations("123")  # Use a dummy user ID for now
    for recommendation in recommendations:
        movie_id = recommendation['movieId']
        # Use the movie ID to get the poster path and title from Elasticsearch
        search_query = {"query": {"match": {"column1": movie_id}}}
        response = client.search(index=movies_table, body=search_query)
        if response['hits']['total']['value'] > 0:
            movie_title = response['hits']['hits'][0]['_source']['column2']
            # You can also get the poster_path here if needed
            movie_data.append({"movie_title": movie_title})
    return jsonify(movie_data)

def get_poster_path(tmdb_id):
    """Gets the poster path of a movie using its tmdbId."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'poster_path' in data and data['poster_path'] is not None:  # Check if poster_path exists and is not None
            return f"https://image.tmdb.org/t/p/w500{data['poster_path']}"
    return None

# Entry point of the Flask application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',8080)))

