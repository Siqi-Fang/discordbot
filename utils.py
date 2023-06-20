import requests
import logging
import pandas as pd
import os

EMAIL = os.environ["EMAIL"]
PASSWORD = os.environ["PASSWORD"]

NAME_2_TRACK = {
    "pixel-penguins": "One Week DS (EST)",
    "algorithmic-armadillos": "One Week DS (PST)",
    "cyborg-cheetahs": "One Week GameDev (PST)",
    "galactic-goblins": "3-Week NLP (EST)",
    "crystal-cyclones": "3-Week NLP (EST)",
    "delta-droids": "3-Week NLP (EST)",
    "energy-emperors": "3-Week NLP (EST)",
    "binary-breakers": "3-Week NLP (EST)",
    "twilight-titans": "3-Week NLP (EST)",
    "lunar-lasers": "3-Week NLP (EST)",
    "turbo-tornadoes": "3-Week CV (EST)",
    "vector-vipers": "3-Week CV (EST)",
    "diamond-dragons": "3-Week CV (EST)",
    "comet-crusaders" :"3-Week CV (EST)",
    "mystic-moons": "3-Week CV (EST)",
    "electron-elephants": "3-Week DS (EST)",
    "kepler-koalas": "3-Week DS (EST)",
    "solar-squirrels": "3-Week DS (EST)",

    "web-wolves": "One Week DS (EST)",
    "protocol-parrots": "One Week DS (EST)",
    "network-nighthawks": "One Week GameDev (EST)",
    "server-snakes": "One Week GameDev (EST)",

    "silicon-starlings": "One Week DS (PST)",
    "firewall-flamingos": "One Week DS (PST)",
    "overclock-owls": "One Week GameDev (PST)",
    "photon-phoenixes": "3-Week NLP (PST)",
    "helix-hawks": "3-Week NLP (PST)",
    "matrix-mustangs": "3-Week NLP (PST)",
    "fusion-flamingos": "3-Week NLP (PST)",
    "omega-otters": "3-Week NLP (PST)",
    "cyberspace-cheetahs": "3-Week CV (PST)",
    "jetstream-jaguars": "3-Week CV (PST)",
    "vortex-vultures": "3-Week CV (PST)",
    "data-dolphins": "3-Week CV (PST)",
    "lumen-lemurs": "3-Week CV (PST)",
    "spectrum-stingrays": "3-Week DS (PST)",
    "kilobit-koalas": "3-Week DS (PST)",
    "radiant-ravens": "3-Week DS (PST)",
    "fractal-firebirds": "3-Week DS (PST)",
    "agile-antelopes": "One Week DS (EST)",
    "brawny-beavers": "One Week DS (EST)",
    "crimson-crocodiles": "One Week GameDev (EST)", 
    "dynamic-dolphins": "One Week DS (PST)",
}

TRACKS = ["One Week DS (EST)",
          "One Week DS (PST)",
          "One Week GameDev (EST)",
          "One Week GameDev (PST)",
          "3-Week DS (EST)",
          "3-Week DS (PST)",
          "3-Week CV (EST)",
          "3-Week CV (PST)",
          "3-Week NLP (EST)",
          "3-Week DS (PST)",
          ]

class MetabaseService:
    '''
    Generic Metabase service class
    '''

    def __init__(self):
        self.session = requests.Session()
        self.logger = logging.getLogger('discord-bot')

    def retrieve_data(self, response):
        request_data_response = response
        request_data = request_data_response.json().get('data')
        request_rows = request_data.get('rows')

        request_columns_raw = request_data.get('cols')
        request_columns = []
        for request_column in request_columns_raw:
            request_columns.append(request_column.get('display_name'))

        request_database = pd.DataFrame(request_rows, columns=request_columns)

        return request_database

    def retrieve(self, id):
        """id is the number in the url of the metabase question you created
           ..../question/<id>/your-question-name
           returns a pandas dataframe
        """
        self.login()
        request_data_response = self.session.post(
            f'https://data.ai-camp.dev/api/card/{id}/query')
        request_database = self.retrieve_data(request_data_response)
        return request_database

    def login(self):
        '''
        Logs into metabase and stays logged in while the session is alive
        '''
        json = {
            'username': EMAIL,
            'password': PASSWORD 
        }
        response = self.session.post(
            'https://data.ai-camp.dev/api/session', json=json)
        if response.status_code == 200:
            token = response.json().get('id')
            self.logger.info(
                'Metabase login successful, new token created and saved')
        else:
            self.logger.error(f'Metabase login failed: {response.text}')

