from flask import Flask
from flask import render_template
import redis

app = Flask(__name__)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/stats/<id>')
def display_detailed_info(id):
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)
	all_keys = r.keys()
        num_entries = len(all_keys)
        not_all_keys = all_keys[:10]
        out = []
        #column_names = ['Labels', 'Total Images', 'Filtered Images', 'Redis Tag Retrieval (seconds)', 'Spark Image Processing (seconds)', 'Copy Found/Not Found', 'Structural Similarity']
        column_names = ['labels','numtotal','filtered','redis','spark','total','found','ssim','url_orig','url_incom']
        for key in all_keys:
                d = {'Key': key}
                val_list = r.lrange(key, 0, -1)
                for i in range(len(column_names)):
                        d[column_names[i]] = val_list[i]
                out.append(d)
        
        return render_template("detailed.html", list_out=out)

@app.route('/stats')
def hello_world():
	r = redis.StrictRedis(host='redis-db.7ptpwl.ng.0001.use1.cache.amazonaws.com', port=6379, db=5)
	all_keys = r.keys()
	num_entries = len(all_keys)
	not_all_keys = all_keys[:10]
	out = []
	#column_names = ['Labels', 'Total Images', 'Filtered Images', 'Redis Tag Retrieval (seconds)', 'Spark Image Processing (seconds)', 'Copy Found/Not Found', 'Structural Similarity']
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
