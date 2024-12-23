from flask import Flask, render_template
from bluprints.analyse import bp_analyse
from entry_to_db import read_and_save_neo4j
from init_db import init_neo4j

app = Flask(__name__)

app.register_blueprint(bp_analyse)

@app.route("/")
def index():
    return render_template("index.html")


with app.app_context():
    app.driver =  init_neo4j()

if __name__ == '__main__':
    # with app.app_context():
    #     read_and_save_neo4j()
    app.run(debug=True)
