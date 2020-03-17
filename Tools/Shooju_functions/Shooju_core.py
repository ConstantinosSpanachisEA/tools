from shooju import Connection, Point
import os
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, date, timedelta
import shooju
import re
import pandas as pd

load_dotenv(find_dotenv())


class ShoojuTools(object):
    def __init__(self):
        try:
            self.shooju_api_key = os.getenv("SHOOJU_API_KEY")
            self.shooju_api_user_name = os.getenv("SHOOJU_USER_NAME")
        except Exception as e:
            raise e
        self.sj = Connection(server='https://energyaspects.shooju.com/',
                             user=self.shooju_api_user_name,
                             api_key=self.shooju_api_key)
        self.m = self.sj.mget()

    def get_multiple_series(self, list_of_queries, fields):
        """
        Gets a number of predefined series and transforms them into a dataframe.
        :param list_of_queries: A list containing a number of coma separated series
        :param fields: Which fields are requested, will be "*" if it is all of them
        :return: a dataframe with data
        """

        for query in list_of_queries:
            self.m.get_series(series_query=query, fields=fields)
        list_of_series = [self.m.fetch()]

    def upload_dataframe(self, df, sids, metadata, job_name, data_column_name):
        """
        Gets a dataframe and inputs its values in Shooju using some pre-defined sids and metadata
        :param df: The dataframe containing the information needed to be stored. Needs to have as an index the datetime
                   values of the dataframe.
        :param sids: The series ids that have for each point on the dataframe
        :param metadata: The specific predefined fields for each point
        :param job_name: The job name for this upload
        :param data_column_name: The column in which the data live. Needs to be a string
        :return: 
        """
        df['sids'] = sids
        with self.sj.register_job(job_name, batch_size=10000) as job:
            for index, row in df.iterrows():
                job.put_field(row['sids'], metadata)
                job.put_point(row['sids'], Point(index, row[data_column_name]))
        job.finish()

    def delete_sj_folder(self, query):
        scroller = self.sj.scroll(query, fields=['sid'], max_points=-1, serializer=shooju.points_serializers.pd_series)
        with self.sj.register_job('delete_folder' + datetime.today().strftime('%Y%m%d_%H%M'),
                                  batch_size=10) as job:  # higher batch size reduces network round trips
            for s in scroller:
                if re.search(r"\s", s['fields']['sid']):
                    job.delete(s['fields']['sid'])
                else:
                    continue
            job.finish(submit=True)

    def upload_data(self, job_description, observations, metadata):
        series_list = observations["series_id"].unique().tolist()
        try:
            with self.sj.register_job(job_description, batch_size=10000) as job:
                for series_id in series_list:
                    series = observations[observations["series_id"] == series_id]["value"]
                    series_metadata = metadata[series_id]
                    series_id = re.sub(r" +", "", series_id)
                    job.put_points(
                        series_id=series_id,
                        points=[Point(idx, value) for idx, value in series.to_dict().items()])
                    job.put_fields(
                        series_id=series_id,
                        fields=series_metadata)
        except AssertionError as error:
            raise error

    def get_points_from_sid_into_df(self, sid, date_start, date_finish, number_of_points=-1):
        data = self.sj.get_points(series_id=sid,
                                  date_start=date_start,
                                  date_finish=date_finish,
                                  max_points=number_of_points,
                                  serializer=shooju.pd_series).sort_index()
        return data

    def get_fields_from_sid(self, sid, fields=["*"]):
        metadata = self.sj.get_fields(series_id=sid,
                                      fields=fields)
        return metadata

    def get_multiple_year_daily_max(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby([data.index.month, data.index.day]).max()
        data.drop(index=(2, 29), inplace=True)
        data.index.names = ["Month", "Day"]
        datetime_index = pd.to_datetime(datetime.today().year * 10000 +
                                        data.index.get_level_values("Month") * 100 +
                                        data.index.get_level_values("Day"), format="%Y%m%d")
        data.reset_index(drop=True, inplace=True)
        data.index = datetime_index
        data.index.name = "Datetime"
        return data

    def get_multiple_year_daily_min(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby([data.index.month, data.index.day]).min()
        data.drop(index=(2, 29), inplace=True)
        data.index.names = ["Month", "Day"]
        datetime_index = pd.to_datetime(datetime.today().year * 10000 +
                                        data.index.get_level_values("Month") * 100 +
                                        data.index.get_level_values("Day"), format="%Y%m%d")
        data.reset_index(drop=True, inplace=True)
        data.index = datetime_index
        data.index.name = "Datetime"
        return data

    def get_multiple_year_daily_avg(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby([data.index.month, data.index.day]).mean()
        data.drop(index=(2, 29), inplace=True)
        data.index.names = ["Month", "Day"]
        datetime_index = pd.to_datetime(datetime.today().year * 10000 +
                                        data.index.get_level_values("Month") * 100 +
                                        data.index.get_level_values("Day"), format="%Y%m%d")
        data.reset_index(drop=True, inplace=True)
        data.index = datetime_index
        data.index.name = "Datetime"
        return data

    def get_multiple_year_weekly_max(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby(data.index.week).max()
        return data

    def get_multiple_year_weekly_min(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby(data.index.week).min()
        return data

    def get_multiple_year_weekly_avg(self, sid, date_start, date_finish, number_of_years):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - number_of_years) |
                         (data.index.year < datetime.today().year)]
        data = data.groupby(data.index.week).mean()
        return data

    def get_five_year_daily_avg(self, sid, date_start, date_finish):
        data = self.get_points_from_sid_into_df(sid, date_start, date_finish)
        data = data.iloc[(data.index.year >= datetime.today().year - 5) | (data.index.year < datetime.today().year)]
        data = data.groupby(data.index.week).mean()
        data.drop(index=(2, 29), inplace=True)
        data.index.names = ["Month", "Day"]
        datetime_index = pd.to_datetime(datetime.today().year * 10000 +
                                        data.index.get_level_values("Month") * 100 +
                                        data.index.get_level_values("Day"), format="%Y%m%d")
        data.reset_index(drop=True, inplace=True)
        data.index = datetime_index
        data.index.name = "Datetime"
        return data

    def create_multiple_query(self, sid, ):
        scroller = self.sj.scroll(query=sid,
                                  fields=['sid'],
                                  max_points=-1,
                                  serializer=shooju.points_serializers.pd_series)
        query = "="
        for s in scroller:
            query = query + "{{sid=" + s["fields"]["sid"] + "}}+"
        text_file = open("query.txt", "w")
        text_file.write(query)
        text_file.close()

    def replace_existing_fields(self, sid, metadata):
        with self.sj.register_job(f"Replacing metadata for {sid}", batch_size=1000) as job:
            job.remove_fields(series_id=sid)
            job.submit()
            job.put_fields(series_id=sid,
                           fields=metadata)
            job.finish(submit=True)

    def copy_data_from_one_sid_to_the_other(self, folder_from, folder_to, delete_previous_sids=False):
        scroller = self.sj.scroll(folder_from, fields=['sid'], max_points=-1,
                                  serializer=shooju.points_serializers.pd_series)
        with self.sj.register_job('delete_folder' + datetime.today().strftime('%Y%m%d_%H%M'),
                                  batch_size=10) as job:  # higher batch size reduces network round trips
            for s in scroller:
                series = self.get_points_from_sid_into_df(s['fields']['sid'], "MIN", "MAX", -1)
                metadata = self.get_fields_from_sid(s['fields']['sid'])
                sid = s['fields']['sid'].replace(folder_from, "").replace("\\", "")
                sid = folder_to + sid
                job.put_fields(series_id=sid,
                               fields=metadata)
                job.put_points(series_id=sid,
                               points=[Point(idx, value) for idx, value in series.to_dict().items()])
                if delete_previous_sids:
                    job.delete(s['fields']['sid'])
        job.finish(submit=True)

    def add_sids_together(self, folder_from, function, operators="", from_list=False):
        if from_list:
            scroller = folder_from
            counter = 0
            for s in scroller:
                if counter == 0:
                    query = '=({{sid="' + s + f'"{operators}' + '}})'
                    counter += 1
                else:
                    query = query + f".{function}" + '({{sid="' + s + f'"{operators}' + \
                            '}}, axis=0, fill_value=0)'
        else:
            scroller = self.sj.scroll(folder_from, fields=['sid'], max_points=-1,
                                      serializer=shooju.points_serializers.pd_series)
            counter = 0
            for s in scroller:
                if counter == 0:
                    query = '=({{sid="' + f"""{s["fields"]["sid"]}""" + f'"{operators}' + '}})'
                    counter += 1
                else:
                    query = query + f".{function}" + '({{sid="' + f"""{s["fields"]["sid"]}""" + f'"{operators}' + \
                            '}}, axis=0, fill_value=0)'
        return query

    def create_y_over_y_change(self, query):
        scroller = self.sj.scroll(query, fields=["sid"], max_points=1, serializer=shooju.points_serializers.pd_series)
        counter = 0
        for s in scroller:
            if counter == 0:
                query = "=({{sid=" + \
                        s["fields"]["sid"] + \
                        "}}).subtract({{" + \
                        s["fields"]["sid"] + \
                        "@lag:1y}}, axis=0 ,fill_value=0)"
                counter += 1
            else:
                query = query + \
                        ".add({{sid=" + \
                        s["fields"]["sid"] + \
                        "}}, axis=0, fill_value=0).subtract({{" + \
                        s["fields"]["sid"] + \
                        "@lag:1y}}, axis=0 ,fill_value=0)"
        return query


if __name__ == '__main__':
    tool = ShoojuTools()
    list_of_q = [
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=France))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Greece))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Italy))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Lithuania))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Netherlands))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Poland))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Portugal))",
        r"((sid=gie\alsi_storage\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_name=Spain))",
        # "((sid=gie\\alsi_storage\\*) AND (source_obj.aspect=sendOut) AND (source_obj.country_short=GB))"
    ]
    for query in list_of_q:
        q = tool.create_y_over_y_change(query)
        print(q)
    #    list = [r"NATIONALGRID\SUPPLIES\38_13740",
    # r"NATIONALGRID\SUPPLIES\38_13724",
    # r"NATIONALGRID\SUPPLIES\38_13728",
    # r"NATIONALGRID\SUPPLIES\38_13723",
    # r"sid=NATIONALGRID\SUPPLIES\38_13651",
    # r"sid=NATIONALGRID\SUPPLIES\38_13734",
    # r"sid=NATIONALGRID\SUPPLIES\38_13653",
    # r"sid=NATIONALGRID\SUPPLIES\38_13689",
    # r"sid=NATIONALGRID\SUPPLIES\38_13736",
    # r"sid=NATIONALGRID\SUPPLIES\38_13592",
    # r"sid=NATIONALGRID\SUPPLIES\38_13693",
    # r"sid=NATIONALGRID\SUPPLIES\38_13715",
    # r"sid=NATIONALGRID\SUPPLIES\38_13716",
    # r"sid=NATIONALGRID\SUPPLIES\38_12704",
    # r"sid=NATIONALGRID\SUPPLIES\38_13713"
    # ]
    #    list = [i.replace("sid=", "") for i in list]
    #    print(len(
    #        list))
    #
    #    query = tool.add_sids_together(list, function="add", operators="@A:d", from_list=True)
    #    print(query)
    #
    #    tool.delete_sj_folder(r"users\constantinos.spanachis\algerian_603_prices")
    # tool.get_points_from_sid_into_df(r"ENTSOG\transport_point\FR-TSO-0003\Obergailbach (FR) / Medelsheim (DE)\entry\Physical Flow", "MIN", "MAX")
    # print("HEY")
    """Used for deleting only things that startwith a pattern in a folder"""

    # scroller = tool.sj.scroll(r"users\constantinos.spanachis", fields=['sid'], max_points=-1, serializer=shooju.points_serializers.pd_series)
    # with tool.sj.register_job('delete_folder' + datetime.today().strftime('%Y%m%d_%H%M'),
    #                           batch_size=10) as job:  # higher batch size reduces network round trips
    #     for s in scroller:
    #         if s["fields"]["sid"].startswith("users\constantinos.spanachis\Damborice"):
    #             job.delete(s["fields"]["sid"])
    # job.finish(submit=True)

    """Used for storing the katharina/etzel Gazprom data in Shooju"""

    # df = pd.read_excel(r"etzel_data.xlsx")
    # df.set_index("Date", drop=True, inplace=True)
    # df.index = pd.to_datetime(df.index)
    # df = df * 95 * 10**(-9)
    # eic = "21Z000000000291I"
    # country_name = "Germany"
    # country_short = "DE"
    # company_name = "21X000000001080H"
    # facility_name ="Gas Storage UGS Etzel EKB"
    # with tool.sj.register_job("Etzel historical data", batch_size=10000) as job:
    #     for attribute in df.columns:
    #         series = df[attribute]
    #         description = re.sub(r'\B(?=[A-Z])', r' ', attribute)
    #         source_obj_attribute = attribute[0].lower() + attribute[1:]
    #         sid = f"""users\\constantinos.spanachis\\gasprom\\{facility_name.replace(" ", "_")}_{attribute}"""
    #         metadata = {"description": f"GIE AGSI Storage by Facility: Gaspool UGS Etzel {description}",
    #                     "source_obj.attribute": source_obj_attribute,
    #                     "source_obj.company": company_name,
    #                     "source_obj.country_name": country_name,
    #                     "soource_obj.eic": eic,
    #                     "source_obj.facility_name": facility_name,
    #                     "source_obj.facility_slug": country_short,
    #                     "source_obj.region_name": "Europe",
    #                     "units": "mcm"
    #                     }
    #         job.put_fields(series_id=sid,
    #                        fields=metadata)
    #         job.put_points(series_id=sid,
    #                        points=[Point(idx, value) for idx,value in series.to_dict().items()])
    #     job.finish()
    #

    """Changing the GASSCO metadata and labels"""
    # for label in ["Dornum", "Dunkerque", "Easington", "Emden", "Fields Delivering into SEGAL", "St. Fergus",
    #               "Zeebrugge", "Aggregated Entry Flow", "Aggregated Exit Flow", "Other Exit Flows", "Fields"]:
    #     metadata = {
    #         'country': "Norway",
    #         "country_iso": "NO",
    #         "region": "EUR",
    #         "energy_production": "natural_gas",
    #         "unit": "mcm",
    #         "source": "GASSCO",
    #         "frequency": "intraday",
    #         "economic_property": "exports",
    #         "economic_property_subtype": "pipeline",
    #         "detail": "actual_volumes",
    #         "lifecycle_stage": "actual",
    #         "measure_point": label,
    #     }
    #     if label in ["Aggregated Exit Flow", "Aggregated Entry Flow",
    #                  "Other Exit Flows", "System Flow Balance"]:
    #         metadata["ea_data_service"] = "no"
    #     else:
    #         metadata["ea_data_service"] = "natural_gas"
    #     sid = f"""gassco\\{label.replace(" ", "_").replace(".", "")}"""
    #     data = tool.sj.get_points(f"gassco\\{label}", "MAX", "MIN", -1, serializer=shooju.pd_series).rename("Value")
    #     with tool.sj.register_job("Gassco Reformating", batch_size=10000) as job:
    #         job.remove_fields(series_id=sid)
    #         job.submit()
    #         job.put_fields(series_id=sid,
    #                        fields=metadata)
    #         job.put_points(sid, [Point(idx, value) for idx, value in data.to_dict().items()])
    #         job.finish(submit=True)

    # for label in ["Graph"]:
    #      metadata = {
    #         'country': "Norway",
    #         "country_iso": "NO",
    #         "region": "EUR",
    #         "energy_production": "natural_gas",
    #         "unit": "mcm",
    #         "source": "GASSCO",
    #         "frequency": "daily",
    #         "economic_property": "production",
    #         "detail": "unaivailability",
    #         "lifecycle_stage": "actual",
    #         "ea_data_service": "natural_gas"
    #      }
    #      data = tool.sj.get_points(f"gassco\\{label}", "MAX", "MIN", -1, serializer=shooju.pd_series).rename("Value")
    #      if label == "Fields":
    #          new_sid = "gassco\\unavailability_fields_processing_plants"
    #      else:
    #          new_sid = "gassco\\unavailability_terminals_exit_points"
    #      with tool.sj.register_job("Gassco Reformating", batch_size=10000) as job:
    #         job.remove_fields(series_id=new_sid)
    #         job.submit()
    #         job.put_fields(series_id=new_sid,
    #                        fields=metadata)
    #         job.put_points(new_sid, [Point(idx, value) for idx, value in data.to_dict().items()])
    #         job.finish(submit=True)

    """Deleting unwanted Gassco Sids"""
    # labels = ["Fields Delivering into SEGAL", "St. Fergus", "Aggregated Entry Flow",
    #           "Aggregated Exit Flow", "Other Exit Flows", "Fields", "Graph"]
    # with tool.sj.register_job("Deleting unwanted Gassco indices", batch_size = 100) as job:
    #     for label in labels:
    #         sid = f"gassco\\{label}"
    #         job.delete(sid)
    #     job.finish()
    #
    #
    #

    """Fixing the mistakes with Gassco"""
    # for label in ["Fields Delivering into SEGAL", "Aggregated Exit Flow", "Aggregated Entry Flow",
    #               "Other Exit Flows", "System Flow Balance"]:
    #     sid = f"gassco\\{label}"
    #     series = tool.sj.get_points("gassco\St. Fergus", "MIN", "MAX", -1, serializer=shooju.pd_series).sort_index()
    #     metadata = {
    #         'country': "Norway",
    #         "country_iso": "NO",
    #         "region": "EUR",
    #         "energy_production": "natural_gas",
    #         "unit": "mcm",
    #         "source": "GASSCO",
    #         "frequency": "intraday",
    #         "economic_property": "exports",
    #         "economic_property_subtype": "pipeline",
    #         "detail": "actual_volumes",
    #         "lifecycle_stage": "actual",
    #         "measure_point": label,
    #     }
    #     if label in ["Aggregated Exit Flow", "Aggregated Entry Flow",
    #                  "Other Exit Flows", "System Flow Balance"]:
    #         metadata["ea_data_service"] = "no"
    #     else:
    #         metadata["ea_data_service"] = "natural_gas"
    #     with tool.sj.register_job('import Historic GASSCO', batch_size=10000) as job:
    #         print(label)
    #         sid = f"""gassco\\{label.replace(" ", "_").replace(".", "")}"""
    #         print(sid)
    #         job.put_fields(sid, metadata)
    #         job.put_points(sid, [Point(dt, value) for dt, value in series.to_dict().items()])
    # job.finish(submit=True)
