import os

from flask import Flask, render_template, send_file


def create_app():
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'lpkit.sqlite'),
    )

    isConfigLoaded = app.config.from_pyfile(os.path.join(app.root_path, 'config.py'),
                                        silent=False)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass


    # databse connection
    from . import db
    db.init_app(app)


    from .apis import ReportApis
    from .configkeeper import ConfigKeeper

    # routers
    @app.route('/')
    def hello():
        return render_template("base.html", reportData=None)
    
    @app.route('/trigger_report/')
    @app.route('/trigger_report/<int:pivotepoch>')
    def triggerReport(pivotepoch:int|None=None):
        reportData = ReportApis.generate(pivotepoch)
        return render_template("base.html", reportData=reportData)
    
    @app.route('/get_report/<reportId>')
    def getReport(reportId:str):
        reportData = ReportApis.get(str(reportId))
        return render_template("base.html", reportData=reportData)

    @app.route('/download_report/<filename>')
    def downloadReport(filename:str):
        return send_file(f"{ConfigKeeper.getReportFolderPath()}{filename}.csv", as_attachment=True)
    
    return app