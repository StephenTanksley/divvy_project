# divvy_project
This is a project which will contain the code for my Divvy bikeshare visualization project. 
This project takes data from the Divvy bikeshare open data initiative and uses it to model impacts of increased bicycle adoption and some current benefits gained at current levels of cycling.

## Steps to Build

1) Create appropriate database in Docker -- this project depends locally on a PostgreSQL database set up in Docker. To initialize locally, run these commands:  
        
        docker volume create postgres_volume

        docker run --name pg_db \
            -e POSTGRES_PASSWORD=<my_secure_password_123>  \
            -v postgres_volume:/var/lib/postgresql/data \
            -d -p 5432:5432 \
            postgres

2) Create a virtual environment -- For anybody who's old hat with Python, you know the drill. Create your virtual environment either with `pip` or with your shiny new `uv` install.

    For convenience, I like to do my virtualenvs the good old fashioned way - `python3 -m venv divvy_project_env`. Then once you're in the root of the `divvy_project` directory, run `source divvy_project_env/bin/activate` to activate the env. `deactivate` to deactivate it.

3) Install requirements.txt -- `uv` provides a `pip`-like interface, so it should be easy enough. While the virtual environment is activated, run `pip install -r requirements.txt`. If using `uv`, you can run `uv add -r requirements.txt` to manage dependencies through `uv` specifically.

4) Usage of components -- There are three main pieces to this project
    1) extract.py -- this is pointed at the S3 bucket for the Divvy bike share service. Run this once a month to retrieve new data. When located in the project directory, use command `python3 extract.py`

    2) transform.py -- this will scan the created directory where the CSV files are located and will load them into the database, tagging them with a new `source_file_id` as appropriate. Run the command `python3 transform.py` to load new rows into the database.

    3) streamlit_integration.py -- with data extracted and loaded into the database, run the command `streamlit run streamlit_integration.py` to load the Streamlit app in a new browser window.