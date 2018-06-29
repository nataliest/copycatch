from flask import Flask
from flask import render_template
import redis

app = Flask(__name__)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/stats/<img_id>')
def display_detailed_info(img_id):
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)
    d_out = {'key': img_id}
    json_key_names = ['labels','numtotal','filtered','redis','spark','total','found','ssim','url_orig','url_incom']
    val_list = r.lrange(img_id, 0, -1)
    for i in range(len(json_key_names)):
            d_out[json_key_names[i]] = val_list[i]
    return render_template("detailed.html", json_out=d_out)

@app.route('/stats')
def hello_world():
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)
	all_keys = r.keys()
	num_entries = len(all_keys)
	out = []
	column_names = ['labels','numtotal','filtered','redis','spark','total','found','ssim']
        for key in all_keys:
		d = {'Key': key}
		val_list = r.lrange(key, 0, -1)
		for i in range(len(column_names)):
			d[column_names[i]] = val_list[i]
		out.append(d)
	
	return render_template("stats.html", list_out=out)

if __name__ == '__main__':
	app.run()
