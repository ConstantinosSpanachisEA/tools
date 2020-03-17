import pandas as pd
import os
import plotly.graph_objects as go
from Tools.Shooju_functions import Shooju_core
import calendar
from datetime import datetime


class PlottingData:
    def __init__(self):
        self.figure_settings = None
        self.figure_data = None

    def Scatter(self, x_data, y_data, figure_layout=dict()):
        data = go.Scatter(x=x_data, y=y_data, mode='lines+markers')
        figure = go.Figure(data=data, layout=figure_layout)
        figure.show()


if "__main__" == __name__:
    plotter = PlottingData()
    downloader = Shooju_core.ShoojuTools()
    figure = go.Figure()
    df = downloader.get_multiple_year_daily_avg(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    x_data = [str(j) + " " + calendar.month_abbr[i] for i, j in df.index.to_list()]
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_average", mode="lines+markers"))
    df = downloader.get_points_from_sid_into_df(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                                date_start="MAX",
                                                date_finish="MIN")
    df_1 = df.iloc[df.index.year == datetime.today().year]
    dates = [(i.month, i.day) for i in df_1.index]
    x_data = [str(j) + " " + calendar.month_abbr[i] for i, j in dates]
    y_data = df_1.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="Current Year", mode="lines+markers",line_width=1.5))
    df = df.iloc[df.index.year == (datetime.today().year - 1)]
    dates = [(i.month, i.day) for i in df.index]
    x_data = [str(j) + " " + calendar.month_abbr[i] for i, j in dates]
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="Previous Year", mode="lines+markers", line_width=1.5))
    df = downloader.get_multiple_year_daily_max(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    x_data = [str(j) + " " + calendar.month_abbr[i] for i, j in df.index.to_list()]
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_max", mode="lines+markers", fill='tonexty', line_color='grey'))
    df = downloader.get_multiple_year_daily_min(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    x_data = [str(j) + " " + calendar.month_abbr[i] for i, j in df.index.to_list()]
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_min", mode="lines+markers", fill='tonexty', line_color='grey'))
    figure.show()


    figure = go.Figure()
    df = downloader.get_multiple_year_weekly_avg(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    x_data = df.index
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_average", mode="lines+markers"))
    df = downloader.get_points_from_sid_into_df(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                                date_start="MAX",
                                                date_finish="MIN")
    df_1 = df.iloc[df.index.year == datetime.today().year]
    df_1 = df_1.groupby(df_1.index.week).mean()
    x_data = df_1.index
    y_data = df_1.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="Current Year", mode="lines+markers",line_width=1.5))
    df = df.iloc[df.index.year == (datetime.today().year - 1)]
    df = df.groupby(df.index.week).mean()
    x_data = df.index
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="Previous Year", mode="lines+markers", line_width=1.5))
    df = downloader.get_multiple_year_weekly_max(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_max", mode="lines+markers", fill='tonexty', line_color='grey'))
    df = downloader.get_multiple_year_weekly_min(sid=r"teams\natural_gas\EUR\GB_storage_underground_stock_level_mcm",
                                          date_start="MAX",
                                          date_finish="MIN",
                                          number_of_years=5)
    y_data = df.to_list()
    figure.add_trace(go.Scatter(x=x_data, y=y_data, name="5yr_min", mode="lines+markers", fill='tonexty', line_color='grey'))
    figure.show()
