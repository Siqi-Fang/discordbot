import requests
import logging
import pandas as pd
import os

EMAIL = os.environ["EMAIL"]
PASSWORD = os.environ["PASSWORD"]

TRACKS = ["One Week DS (EST)",
          "One Week DS (PST)",
          "3-Week CV (EST)",
          "3-Week NLP (EST)",
          "3-Week NLP (PST)",
          ]
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

    "majestic-meerkats": "One Week GameDev (PST)",
    "whimsical-wolves": "One Week DS (PST)",
    "zesty-zebras": "One Week DS (PST)",
    "bubbly-butterflies": "One Week GameDev (EST)",
    "vivacious-vultures": "One Week DS (EST)",
    "whirlwind-walruses": "One Week GameDev (EST)",
    "stellar-stingrays": "One Week DS (EST)",
# BATCH B
    "crazy-centipedes": "One Week DS (EST)",

    "resilient-raccoons": "3-Week DS (EST)",
    "humble-hedgehogs": "3-Week DS (EST)",
    "openminded-octopuses": "3-Week DS (EST)",

    "passionate-pigeons": "3-Week DS (PST)",
    "wandering-woodpeckers": "3-Week DS (PST)",
    "dynamic-dalmations": "3-Week DS (PST)",
    "intelligent-irukandji": "3-Week DS (PST)",
    "witty-wombats": "3-Week DS (PST)",
    "quirky-quesadillas": "3-Week DS (PST)",

    "fabulous-flamingos": "3-Week NLP (EST)",
    "jolly-jackrabbits": "3-Week NLP (EST)",
    "lovely-llamas": "3-Week NLP (EST)",
    "peaceful-pandas": "3-Week NLP (EST)",
    "sensible-seahorses":"3-Week NLP (EST)",
    "harmonic-hawks": "3-Week NLP (EST)",
    "joyous-jackals": "3-Week NLP (EST)",
    "noteworthy-nuthatches": "3-Week NLP (EST)",

    "sincere-swallows": "3-Week NLP (PST)",
    "mystical-manatees": "3-Week NLP (PST)",
    "gentle-giraffes": "3-Week NLP (PST)",
    "fabulous-fajitas": "3-Week NLP (PST)",
    "proud-porcupines":"3-Week NLP (PST)",

    "gleeful-geese": "3-Week CV (EST)",
    "merry-mockingbirds": "3-Week CV (EST)",
    "cheerful-cheetahs": "3-Week CV (EST)",
    "ambitious-aardvarks": "3-Week CV (EST)",
    "dapper-dingoes": "3-Week CV (EST)",

    "feist-finches": "3-Week CV (PST)",
    "lovable-lobsters": "3-Week CV (PST)",
    "curious-caterpillars": "3-Week CV (PST)",
    "brilliant-butterflies": "3-Week CV (PST)",
    "radiant-roadrunners": "3-Week CV (PST)",
    #B2
    "unforgettable-udon": "One Week DS (EST)",
    "magnificent-macaroons": "One Week DS (EST)",
    "daring-dumplings":"One Week DS (EST)",
    "happy-hamburgers": "One Week DS (PST)",
    "bountiful-bagels": "One Week DS (PST)",
    "toasty-tangerine":"One Week DS (PST)",
    # B3
    "hungry-hashbrowns": "One Week DS (EST)",
    "jolly-jalapenos": "One Week GameDev (EST)",
    "whimsical-watermelons": "One Week DS (PST)",
    "giggly-gummybears": "One Week GameDev (EST)",

    # BATCH C 
    'fury-falcons': "Expert Course (EST)",
    'roaring-ravens': "Expert Course (EST)",
    'valkyrie-vipers': "Expert Course (PST)",
    'cyclone-cougars' :"Expert Course (PST)",
    'silver-spartans': "Expert Course (PST)", 
    'monarch-mammoths': "One Week DS (PST)",
    'magical-macaronis': "3-Week NLP (PST)",
    'swift-swans': "3-Week DS (PST)",
    'remarkable-ravioli': "3-Week DS (PST)",
    'prismatic-piccolos': "3-Week CV (PST)",
    'marvelous-mangos': "3-Week CV (PST)",
    'tuneful-trombones': "3-Week NLP (PST)",
    'mystical-mandolins': "3-Week NLP (PST)",
    'quality-quesadillas': "3-Week CV (PST)",
    'legendary-lasagnas': "3-Week CV (PST)",
    'laughing-llamas': "3-Week CV (PST)",
    'rhythmic-recorders': "3-Week NLP (PST)", 
    'curious-chickens': "3-Week NLP (EST)",
    'bouncing-blueberries': "3-Week NLP (EST)",
    'sweet-strawberries': "3-Week NLP (EST)",
    'amazing-avocados': "3-Week CV (EST)",
    'charming-cherries': "3-Week CV (EST)",
    'fearless-flamingos': "3-Week CV (EST)",
    'exquisite-eclairs': "3-Week CV (EST)",
    'astonishing-almonds': "3-Week DS (EST)",
    'tasty-tacos': "3-Week DS (EST)",
    'bold-badgers': "3-Week DS (EST)",
    'energetic-emus': "3-Week NCV (EST)",
    "keen-kiwis": "3-Week NLP (EST)",
    'generous-grapefruits': "3-Week NLP (EST)",
    'dizzy-doughnut': "One Week DS (EST)",
    'charming-cheesecakes': "One Week DS (EST)",
    'popping-pistachios': "One Week DS (PST)",
    'glittering-gingerbreads': "One Week GameDev (PST)",
    'radiant-raspberries': "One Week DS (EST)",

    'playful-pretzels':"One Week GameDev (PST)",
    'jovial-jujubes': "One Week DS (PST)",
    # D
    "quirky-quinoas": "3-Week NLP (EST)",
    "cozy-cornbreads": "3-Week NLP (EST)",
    "bouncy-baguettes": "3-Week CV (EST)",
    "majestic-mangos": "3-Week CV (EST)",
    "blissful-brownies": "3-Week CV (EST)",
    "blissful-blueberries": "3-Week NLP (PST)",
    "enchanting-espressos": "3-Week NLP (PST)",
    "captivating-cupcakes": "3-Week NLP (PST)",
    "whimsical-wonton": "One Week DS (EST)",
    "fancy-fondue":"One Week DS (PST)"
    
}

CURRENT_CLASSES = [
    # Batch D
    "quirky-quinoas",
    "cozy-cornbreads",
    "bouncy-baguettes",
    "majestic-mangos",
    "blissful-brownies",
    "blissful-blueberries",
    "enchanting-espressos",
    "captivating-cupcakes",
    # D1
    "whimsical-wonton",
    "fancy-fondue"
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

