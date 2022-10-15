import dash
import plotly_express as px
from dash import dcc
from dash import html
import utils
from dash.dependencies import Input, Output, State

app = dash.Dash(__name__)

#market = 'IBEX'
#ticker = 'SAN'

api_handler = utils.BMEApiHandler()


app.layout = html.Div(
    children=[
        html.H1('Market Data Explorer',style={
            'textAlign': 'center'}),
        html.P('mIAx API',),
        dcc.Dropdown(
            id='market-index',
            options=[
                {'label': 'IBEX', 'value': 'IBEX'},
                {'label': 'DAX', 'value': 'DAX'},
                {'label': 'EUROSTOXX', 'value': 'EUROSTOXX'},
            ],
            value='IBEX'
        ),
        dcc.Dropdown(
            id='menu-ticker'
        ),
        dcc.Graph(
            id='ticker-graph'
        )
    ]
)

@app.callback(
    Output("menu-ticker", "options"),
    Input("market-index", "value")

)
def select_market(market):
    tickers = api_handler.get_ticker_master(market)
    ticker_options = [{'label': tck, 'value': tck} for tck in tickers.ticker]
    return ticker_options

@app.callback(
    Output("menu-ticker", "value"),
    Input("menu-ticker", "options")

)
def select_tck(ticker_options):
    return ticker_options[0]['value']

@app.callback(
    Output("ticker-graph", "figure"),
    State("market-index", "value"),
    Input("menu-ticker", "value")
    

)
def plot_figure(market,ticker):
    data = api_handler.get_ohlc_data(market,ticker)['close']
    fig = px.line(data)
    return fig

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", debug=False, port=8080)


    