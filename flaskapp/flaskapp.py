from flask import Flask
from flask import render_template
import redis

app = Flask(__name__)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/stats/<id>')
def display_detailed_info(id):
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=4)
	key = '/m/' + id
	stuff = "Label is: " + r.get(key) + "!!"
	return render_template("detailed.html", label = stuff)

@app.route('/stats')
def hello_world():
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=4)
	all_keys = r.keys()
	num_entries = len(all_keys[:10])
	not_all_keys = all_keys[:10]
	out = []
	for key in not_all_keys:
		out.append({"Key": key, "val": r.get(key)})
	
	return render_template("stats.html", list_out=out)

if __name__ == '__main__':
	app.run()
