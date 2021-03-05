from chalice import Chalice
from chalicelib.fixtures import fixtures
from chalicelib.historical import history
from chalicelib.match_predictions import predictions

app = Chalice(app_name='football_predictions')
app.register_blueprint(fixtures)
app.register_blueprint(history)
app.register_blueprint(predictions)
