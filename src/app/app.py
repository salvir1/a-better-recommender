from flask import Flask, request, render_template
import numpy as np
import pickle

app = Flask(__name__)

# Form page to submit text
@app.route('/')
def home():
    poster_var = '/uXDfjJbdP4ijW5hWSBrPrlKpxab.jpg'
    link_suffix = '862-toy-story'
    title = "Toy Story"
    show_page = f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width">
        <title>A Better Recommender</title>
      </head>
      <body>
        <h2>Here are ten movie recommendations. We'd like to know how well they align with your tastes.</h2>
         <div>
        <p>How many have you seen and didn't like?</p> <input id="a"></input>
        <p>How many have you seen and liked?</p> <input id="b"></input>
        <p>How many have you not seen, but would like to see?</p> <input id="c"></input>
        <p></p>
    </div>

        <div class='posters' >
            <a href="https://www.themoviedb.org/movie/{link_suffix}">
              <img src='http://image.tmdb.org/t/p/w185/{poster_var}' title={title}>
            </a>
            <a href="https://www.themoviedb.org/movie/{link_suffix}">
              <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
            </a>
            <a href="https://www.themoviedb.org/movie/{link_suffix}">
              <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
            </a>
            <a href="https://www.themoviedb.org/movie/{link_suffix}">
              <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
            </a>
            <a href="https://www.themoviedb.org/movie/{link_suffix}">
              <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
            </a>
        </div>
        <div class='posters' >
        <a href="https://www.themoviedb.org/movie/{link_suffix}">
          <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
        </a>
        <a href="https://www.themoviedb.org/movie/{link_suffix}">
          <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
        </a>
        <a href="https://www.themoviedb.org/movie/{link_suffix}">
          <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
        </a>
        <a href="https://www.themoviedb.org/movie/{link_suffix}">
          <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
        </a>
        <a href="https://www.themoviedb.org/movie/{link_suffix}">
          <img src='http://image.tmdb.org/t/p/w185/{poster_var}'>
        </a>
        </div>



      </body>
    </html>
    """
    return show_page


# def submission_page():
#     return render_template('index.html')

@app.route('/recs')
def recs():
    'http://image.tmdb.org/t/p/w185/'
    return render_template('index.html')

# My prediction app
@app.route('/prediction', methods=['POST'] )
def predict():
    with open("model.pkl", 'rb') as f:
        model = pickle.load(f)
        X = np.array([request.form['sepal_length'],
                      request.form['sepal_width'],
                      request.form['petal_length'],
                      request.form['petal_width']]).astype(float).reshape(1, -1)
        probs = model.predict_proba(X)
    page = f'''Predicted probabilities:
    <table>
        <tr><th>species</th><th>probability</th></tr>
        <tr><td>iris setosa</td><td>{probs[0][0]:.2f}</td></tr>
        <tr><td>iris versicolor</td><td>{probs[0][1]:.2f}</td></tr>
        <tr><td>iris virginica</td><td>{probs[0][2]:.2f}</td></tr>
    </table>'''

    return page

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
