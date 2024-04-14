from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict
from itertools import combinations
import requests


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

# Dictionnaire pour stocker les favoris des utilisateurs
favorites = {}

# Créer une application Flask
app = Flask(__name__)

# Route pour la page d'accueil
@app.route("/")
def home():
    return "Home"

# Route pour l'autocomplétion des titres de film
@app.route("/autocomplete")
def autocomplete():
    # Récupérer le terme de recherche depuis la requête
    query = request.args.get('query')

    # Vérifier si le terme de recherche est vide
    if not query:
        return jsonify([])  # Retourner une liste vide si le terme de recherche est vide

    # Effectuer une recherche Elasticsearch pour obtenir les suggestions d'autocomplétion
    body = {
        "query": {
            "match_phrase_prefix": {
                "column2": {  # Recherche dans la clé 'column2' pour les titres des films
                    "query": query,
                    "max_expansions": 10  # Ajustez le nombre d'expansions au besoin
                }
            }
        }
    }

    response = client.search(index=INDEX_NAME, body=body)
    suggestions = [hit['_source']['column2'] for hit in response['hits']['hits']]  # Récupérer les titres des films
    return jsonify(suggestions)

# Route pour ajouter un film aux favoris
@app.route("/add_favorite", methods=["POST"])
def add_favorite():
    data = request.get_json()
    user_id = data.get("user_id")
    movie_id = data.get("movie_id")
    
    # Vérifier si l'utilisateur existe dans le dictionnaire
    if user_id in favorites:
        # Ajouter le film à la liste de favoris de l'utilisateur existant
        favorites[user_id].append(movie_id)
    else:
        # Créer une nouvelle entrée pour l'utilisateur et ajouter le film
        favorites[user_id] = [movie_id]
    
    return jsonify({"message": "Film ajouté aux favoris avec succès"})

# Route pour récupérer la liste des favoris
@app.route("/get_favorites", methods=["GET"])
def get_favorites():
    user_id = request.args.get("user_id")
    if user_id in favorites:
        return jsonify(favorites[user_id])
    else:
        return jsonify([])  # Retourner une liste vide si l'utilisateur n'a pas de favoris

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

# Fonction pour trouver les utilisateurs similaires
def get_similar_users(target_user_id, ratings_data):
    # Dictionnaire pour stocker les évaluations des films par utilisateur
    user_ratings = defaultdict(set)
    
    # Remplir le dictionnaire avec les évaluations des films par utilisateur
    for user_id, movie_id in ratings_data:
        user_ratings[user_id].add(movie_id)
    
    # Ensemble des films évalués par l'utilisateur cible
    target_user_movies = user_ratings[target_user_id]
    
    # Calculer la similarité de Jaccard entre l'utilisateur cible et chaque autre utilisateur
    similarities = {}
    for user_id, movies in user_ratings.items():
        # Ignorer l'utilisateur cible lui-même
        if user_id == target_user_id:
            continue
        
        # Calculer l'intersection et l'union des ensembles de films
        intersection = len(target_user_movies.intersection(movies))
        union = len(target_user_movies.union(movies))
        
        # Calculer la similarité de Jaccard
        similarity = intersection / union
        
        similarities[user_id] = similarity
    
    # Trier les utilisateurs par similarité en ordre décroissant
    sorted_users = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    
    # Sélectionner les 3 utilisateurs les plus similaires
    top = [{"user_id": user_id, "similarity": similarity} for user_id, similarity in sorted_users[:3]]
    
    return top


def get_recommendations(user_id):
    recommendations = []
    similar_users = get_similar_users()

    # S'assure que les identifiants sont des entiers et non des chaînes de caractères
    subquery = f"""
        SELECT userid
        FROM `{ratings_table}`
        WHERE userid IN ({', '.join(str(user_id) for user_id in similar_users)})
    """

    query = f"""
        SELECT DISTINCT predicted_rating_im_confidence, userId, movieId
        FROM ML.RECOMMEND(MODEL `{model_table}`, (SELECT userid FROM ({subquery})))
        WHERE predicted_rating_im_confidence > 0.75
        ORDER BY predicted_rating_im_confidence DESC
        LIMIT 10
    """
    # Execute the query
    query_job = client_query.query(query)
    
    return recommendations

#List of the reccomended movie titles
@app.route('/display_reccomendations')
def display_recommendations():
    movie_data = []
    # Passer l'ID de l'utilisateur à la fonction de recommandation pour obtenir les recommandations
    recommendations = get_recommendations("123")  # Utilisez un ID d'utilisateur factice pour l'instant
    for recommendation in recommendations:
        movie_id = recommendation['movieId']
        # Utilisez l'ID du film pour obtenir le chemin du poster et le titre à partir de Elasticsearch
        search_query = {"query": {"match": {"column1": movie_id}}}
        response = client.search(index=movies_table, body=search_query)
        if response['hits']['total']['value'] > 0:
            movie_title = response['hits']['hits'][0]['_source']['column2']
            # Vous pouvez également obtenir le poster_path ici si nécessaire
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
    app.run(host='0.0.0.0', port=8080)
