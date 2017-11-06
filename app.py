
from flask import Flask
from flask import app, render_template
import os
import MapModule as mm

app = Flask(__name__)

@app.route('/')
def index():
    obj = mm()
     # make the templates dir
    newpath = r'/opt/app-root/src/templates'
    if not os.path.exists(newpath):
        os.makedirs(newpath, 0o77)
    # move the file to the templates dir
    os.rename('/opt/app-root/src/map.html', '/opt/app-root/src/templates/map.html')
    resp = render_template("map.html", title='Maps')
    return resp

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
