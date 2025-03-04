"""Main app definition."""

import json
import os
from dataclasses import asdict
from pathlib import Path
from urllib.parse import quote

import requests
import structlog
from authlib.integrations.flask_client import OAuth
from flask import Flask, redirect, request, render_template, session, url_for
from flask_htmx import HTMX
from flask_login import LoginManager, login_required, login_user, logout_user
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from frictionless import Package

from parquet_fe_prototype.models import db, User
from parquet_fe_prototype.dash import initialize_dash
from parquet_fe_prototype.duckdb_query import ag_grid_to_duckdb, Filter
from parquet_fe_prototype.search import initialize_index, run_search
from parquet_fe_prototype.utils import clean_descriptions

AUTH0_DOMAIN = os.getenv("PUDL_VIEWER_AUTH0_DOMAIN")
CLIENT_ID = os.getenv("PUDL_VIEWER_AUTH0_CLIENT_ID")
CLIENT_SECRET = os.getenv("PUDL_VIEWER_AUTH0_CLIENT_SECRET")

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()


def __init_auth0(app: Flask):
    """Connects our application to Auth0.

    The client ID, client secret, and auth0 domain are all accessible at
    manage.auth0.com.

    The auth0 object this returns has a bunch of methods that handle the
    various steps of the OAuth flow.
    """
    oauth = OAuth()
    oauth.init_app(app)

    auth0 = oauth.register(
        "auth0",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        server_metadata_url=f"https://{AUTH0_DOMAIN}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid profile email"},
    )
    return auth0


def __init_db(db: SQLAlchemy, app: Flask):
    """Connect application to Postgres database for storing users.

    Uses host/port in development environment, but on Cloud Run we use a Unix
    socket under /cloudsql.
    """
    username = os.getenv("PUDL_VIEWER_DB_USERNAME")
    password = os.getenv("PUDL_VIEWER_DB_PASSWORD")
    database = os.getenv("PUDL_VIEWER_DB_NAME")

    if os.environ.get("IS_CLOUD_RUN"):
        cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")
        db_uri = f"postgresql://{username}:{password}@/{database}?host=/cloudsql/{cloud_sql_connection_name}"
    else:
        host = os.getenv("PUDL_VIEWER_DB_HOST")
        port = os.getenv("PUDL_VIEWER_DB_PORT")
        db_uri = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
    db.init_app(app)

    migrate = Migrate()
    migrate.init_app(app, db)


def __build_search_index(app):
    """Create a search index.

    We currently convert a static YAML file into a Frictionless datapackage,
    then pass that in.
    """
    # TODO: in the future, download this datapackage from nightly build.
    s3_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl_parquet_datapackage.json"

    log.info(f"loading datapackage from {s3_url}")
    datapackage_descriptor = requests.get(s3_url).json()
    datapackage = clean_descriptions(Package.from_descriptor(datapackage_descriptor))
    index = initialize_index(datapackage)
    return datapackage, index


def create_app():
    """Main app definition.

    1. initialize Flask app with a bunch of extensions:
        * auth0 for authentication
        * htmx for simplifying our client/server interaction
        * accessing the db through sql alchemy
        * logins/sessions
    2. set up the search index
    3. define a bunch of application routes
    """
    app = Flask("parquet_fe_prototype", instance_relative_config=True)
    if os.getenv("IS_CLOUD_RUN"):
        app.config["PREFERRED_URL_SCHEME"] = "https"
    app.config.from_mapping(
        SECRET_KEY=os.getenv("PUDL_VIEWER_SECRET_KEY"),
        TEMPLATES_AUTO_RELOAD=True,
        LOGIN_DISABLED=os.getenv("PUDL_VIEWER_LOGIN_DISABLED", False),
    )

    auth0 = __init_auth0(app)

    htmx = HTMX()
    htmx.init_app(app)

    __init_db(db, app)

    login_manager = LoginManager()
    login_manager.init_app(app)

    initialize_dash(app)

    datapackage, index = __build_search_index(app)

    @app.get("/")
    def home():
        """Just a redirect for search until we come up with proper content."""
        return redirect(url_for("search"))

    @login_manager.user_loader
    def __load_user(user_id):
        """Teach Flask-Login how to interact with our Users in db."""
        return User.query.get(int(user_id))

    @app.route("/login")
    def login():
        """Redirect to auth0 to handle actual logging in.

        Params:
            next: the next URL to redirect to once logged in.
        """
        next = request.args.get("next")
        if next:
            redirect_uri = url_for("callback", next=next, _external=True)
        else:
            redirect_uri = url_for("callback", _external=True)
        print(redirect_uri)
        return auth0.authorize_redirect(redirect_uri=redirect_uri)

    @app.route("/callback")
    def callback():
        """Once user successfully logs in on Auth0, it redirects here.

        We want to then log that user in on our system as well since we trust
        Auth0. If they don't exist in our system we add them.

        Params:
          next: the next URL to redirect to once logged in.
        """
        next_url = request.args.get("next", url_for("search"))
        token = auth0.authorize_access_token()
        userinfo = token["userinfo"]
        user = User.query.filter_by(auth0_id=userinfo["sub"]).first()
        if not user:
            user = User.from_userinfo(userinfo)
            db.session.add(user)
            db.session.commit()
        login_user(user, remember=True)
        return redirect(next_url)

    @login_required
    @app.route("/logout")
    def logout():
        """Log out user from our session & auth0 session, then go home."""
        logout_user()
        session.clear()
        return_to = quote(url_for("home", _external=True))
        response = redirect(
            f"https://{AUTH0_DOMAIN}/v2/logout?"
            f"client_id={CLIENT_ID}&"
            f"return_to={return_to}"
        )
        response.delete_cookie("remember_token")
        response.delete_cookie("session")
        return response

    @app.get("/search")
    def search():
        """Run a search query and return results.

        If hit as part of an HTMX request, only render the search results HTML
        fragment. Otherwise render the whole page.

        Params:
            q: the query string
        """
        template = "partials/search_results.html" if htmx else "search.html"
        query = request.args.get("q")
        log.info("search", url=request.path, query=query)

        def sort_resources_by_name(resource):
            name = resource.name
            if name.startswith("out"):
                return 0
            if name.startswith("core"):
                return 1
            if name.startswith("_out"):
                return 2
            if name.startswith("_core"):
                return 3

        if query:
            resources = run_search(ix=index, raw_query=query)
        else:
            resources = sorted(datapackage.resources, key=sort_resources_by_name)

        return render_template(template, resources=resources, query=query)

    @app.get("/api/duckdb")
    def duckdb():
        """Take filters from Perspective and return a DuckDB query.

        Params:
            perspective_filters: a table name and its associated filters.
            forDownload: whether this is for the full download (i.e., no row
                limit) or a sample query which needs a limit to be fast.

        Returns:
            duckdb_query: prepared statements and the corresponding values to
                both query the data and also get a full row-count of the result
                set.
        """
        name = request.args.get("name")
        filters = [
            Filter.model_validate(f)
            for f in json.loads(request.args.get("filters", "[]"))
        ]
        duckdb_query = ag_grid_to_duckdb(name=name, filters=filters)
        page = int(request.args.get("page", 1))
        DEFAULT_PREVIEW_PAGE = 10_000
        DEFAULT_CSV_EXPORT_PAGE = 1_000_000
        per_page = int(request.args.get("perPage", DEFAULT_PREVIEW_PAGE))
        if per_page == DEFAULT_PREVIEW_PAGE:
            event = "duckdb_preview"
        elif per_page == DEFAULT_CSV_EXPORT_PAGE:
            event = "duckdb_csv"
        else:
            event = "duckdb_other"

        log.info(event, url=request.path, params=dict(request.args))
        offset = (page - 1) * per_page
        duckdb_query.statement += f" LIMIT {per_page} OFFSET {offset}"
        return asdict(duckdb_query)

    return app
