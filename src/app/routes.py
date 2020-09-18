from flask import render_template
from app import app

@app.route('/')
@app.route('/home')
def home():
    movie = []
    " Add a Let's Get Started' button to redirect to input page"
    return '''
    <html>
        <head>
            <title>Home Page - Recommender Evaluator</title>
        </head>
        <body>
            <h1>Movie Recommendatation Engine Evaluator</h1>
            <p>Welcome! Thanks for helping me to evaluate different types of movie
            recommendation engines. </p>
            <p>I'll start by asking a few simple questions.</p>
            <p>Then present some recommendations to you.</p>
            <p>And ask you about the recommendations.</p>
            <p>It should be pretty quick, straightforward, and hopefully fun. </p>
            <p>Ready?</p>
        </body>
    </html>
    '''
@app.route('/recommendations')
def recommendations():
    posters = {'poster_var' : '/uXDfjJbdP4ijW5hWSBrPrlKpxab.jpg'}
    links = {'link_suffix' : '862-toy-story'}
    title = {'title' : "Toy Story"}
    return render_template('recommendations.html', posters=posters, links=links, title=title)