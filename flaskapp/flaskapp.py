from flask import Flask
from flask import render_template, redirect
import redis

app = Flask(__name__)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/github')
def mygit():
	return redirect("https://github.com/nataliest/copycatch")

@app.route('/ppt')
def myppt():
	return redirect("https://docs.google.com/presentation/d/10VAHEpHDFsiSNJHW4r9R7T4DvutFJHCB0-IHHuC_ksk/edit?usp=sharing")

@app.route('/tech')
def pipeline():
	pipeline = "https://raw.githubusercontent.com/nataliest/copycatch/master/docs/CopyCatch_pipeline.png"
	return render_template("tech.html", pipeline=pipeline)

@app.route('/linkedin')
def mylink():
	return redirect("https://www.linkedin.com/in/natalie-lebedeva/")

@app.route('/about')
def about_me():
	img_link = "https://raw.githubusercontent.com/nataliest/copycatch/master/flaskapp/assets/aboutme.JPG"
	return render_template("about.html", img_link = img_link)

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
