from flask import Flask, request
from recommender import get_product_recommendation, show_movies
from flask_cors import CORS
import json

app =Flask(__name__)
CORS(app)

@app.route("/generate-movie")
def generate_movie():

    return show_movies(20)


@app.route("/get-recommendation", methods=['POST'])
def get_recommendation():

    getData = json.loads(request.data)
    clickedId = getData['id']
    print(clickedId, type(clickedId))
    return get_product_recommendation(clickedId, 5)

if __name__ == "__main__":

    # debug true just for development
    app.run(debug=True)





