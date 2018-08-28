from flask import Flask
from flask import render_template, redirect

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
	d = {'4956e49d069d7fa5': ['Invertebrate, Dragonfly', '1868440', '1064', '0.11', '33.94', '34.53', 'Not found', '8.39%', 'https://s3.amazonaws.com/copycatch-web-pics/4956e49d069d7fa5.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0.jpg'], 'img078d8f3c6770de4a': ['Flower, Plant', '1868443', '51014', '1.48', '384.65', '386.3', 'Found', '74.77%', 'https://s3.amazonaws.com/copycatch-web-pics/img078d8f3c6770de4a.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/078d8f3c6770de4awm.jpg'], 'img01f92a05010cabe0': ['Snack, Baked goods, Food, Cookie', '1868441', '636', '0.53', '29.28', '29.91', 'Found', '100.0%', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg'], '9bceca9268f4aa33': ['Monkey, Animal, Mammal', '1868442', '234', '0.71', '22.29', '23.54', 'Not found', '7.17%', 'https://s3.amazonaws.com/copycatch-web-pics/9bceca9268f4aa33.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/0739c6aeefaacead.jpg'], 'ec3de8b2009536a9': ['Snack, Baked goods, Food, Cookie', '1868441', '635', '0.48', '28.07', '28.91', 'Not found', '8.74%', 'https://s3.amazonaws.com/copycatch-web-pics/ec3de8b2009536a9.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg'], '01b95ffb7ee0752c': ['Vehicle, Airplane, Aircraft', '1868443', '10681', '1.06', '97.8', '99.05', 'Found', '90.58%', 'https://s3.amazonaws.com/copycatch-web-pics/img01b95ffb7ee0752cwm.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg'], 'img01b95ffb7ee0752cwm': ['Vehicle, Airplane, Aircraft', '1868443', '10681', '1.05', '103.61', '104.75', 'Found', '100.0%', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg', 'Vehicle, Airplane, Aircraft', '1868443', '10681', '1.05', '99.33', '100.51', 'Found', '90.58%', 'https://s3.amazonaws.com/copycatch-web-pics/img01b95ffb7ee0752cwm.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg'], 'img01cec4b708afe6b0': ['Invertebrate, Dragonfly', '1868440', '1065', '0.12', '28.57', '28.87', 'Found', '97.23%', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0wm.jpg']}
	d_out = {'key': img_id}
	json_key_names = ['labels','numtotal','filtered','redis','spark','total','found','ssim','url_orig','url_incom']
	val_list = d[img_id]
	for i in range(len(json_key_names)):
		d_out[json_key_names[i]] = val_list[i]
	return render_template("detailed.html", json_out=d_out)

@app.route('/stats')
def hello_world():
	out = []
	d = {'4956e49d069d7fa5': ['Invertebrate, Dragonfly', '1868440', '1064', '0.11', '33.94', '34.53', 'Not found', '8.39%', 'https://s3.amazonaws.com/copycatch-web-pics/4956e49d069d7fa5.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0.jpg'], 'img078d8f3c6770de4a': ['Flower, Plant', '1868443', '51014', '1.48', '384.65', '386.3', 'Found', '74.77%', 'https://s3.amazonaws.com/copycatch-web-pics/img078d8f3c6770de4a.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/078d8f3c6770de4awm.jpg'], 'img01f92a05010cabe0': ['Snack, Baked goods, Food, Cookie', '1868441', '636', '0.53', '29.28', '29.91', 'Found', '100.0%', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg'], '9bceca9268f4aa33': ['Monkey, Animal, Mammal', '1868442', '234', '0.71', '22.29', '23.54', 'Not found', '7.17%', 'https://s3.amazonaws.com/copycatch-web-pics/9bceca9268f4aa33.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/0739c6aeefaacead.jpg'], 'ec3de8b2009536a9': ['Snack, Baked goods, Food, Cookie', '1868441', '635', '0.48', '28.07', '28.91', 'Not found', '8.74%', 'https://s3.amazonaws.com/copycatch-web-pics/ec3de8b2009536a9.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/img01f92a05010cabe0.jpg'], '01b95ffb7ee0752c': ['Vehicle, Airplane, Aircraft', '1868443', '10681', '1.06', '97.8', '99.05', 'Found', '90.58%', 'https://s3.amazonaws.com/copycatch-web-pics/img01b95ffb7ee0752cwm.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg'], 'img01b95ffb7ee0752cwm': ['Vehicle, Airplane, Aircraft', '1868443', '10681', '1.05', '103.61', '104.75', 'Found', '100.0%', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg', 'Vehicle, Airplane, Aircraft', '1868443', '10681', '1.05', '99.33', '100.51', 'Found', '90.58%', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01b95ffb7ee0752c.jpg'], 'img01cec4b708afe6b0': ['Invertebrate, Dragonfly', '1868440', '1065', '0.12', '28.57', '28.87', 'Found', '97.23%', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0.jpg', 'https://s3.amazonaws.com/copycatch-web-pics/01cec4b708afe6b0wm.jpg']}
	column_names = ['labels','numtotal','filtered','redis','spark','total','found','ssim']
        for key in d:
		dic = {'Key': key}
		#val_list = r.lrange(key, 0, -1)
		val_list = d[key]
		for i in range(len(column_names)):
			dic[column_names[i]] = val_list[i]
		out.append(dic)
	
	return render_template("stats.html", list_out=out)

if __name__ == '__main__':
	app.run()
