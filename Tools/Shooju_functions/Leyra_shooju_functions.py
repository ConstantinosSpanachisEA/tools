from shooju import Connection, Point
import os
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, date, timedelta
import shooju

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

    def delete_sj_folder(self, query, **kwargs):
        scroller = self.sj.scroll(query, fields=['sid'],
                                  max_points=-1,
                                  serializer=shooju.points_serializers.pd_series,
                                  **kwargs)
        with self.sj.register_job('delete_folder' + datetime.today().strftime('%Y%m%d_%H%M'),
                                  batch_size=10000) as job:  # higher batch size reduces network round trips
            for s in scroller:
                job.delete(s['fields']['sid'])
            job.finish(submit=True)

    def get_points_from_sid_into_df(self, sid, date_start, date_finish, number_of_points=-1, return_csv=True ):
        data = self.sj.get_points(series_id=sid,
                                  date_start=date_start,
                                  date_finish=date_finish,
                                  max_points=number_of_points,
                                  serializer=shooju.pd_series).sort_index()
        if return_csv:
            data.to_csv("data_requested.csv")
        else:
            return data

    def add_sids_together(self, folder_from, function):
        scroller = self.sj.scroll(folder_from, fields=['sid'], max_points=-1,
                                  serializer=shooju.points_serializers.pd_series)
        counter = 0
        for s in scroller:
            if counter == 0:
                query = "=({{sid=" + f"""{s["fields"]["sid"]}""" + "}})"
                counter += 1
            query = query + f".{function}" + "({{sid=" + f"""{s["fields"]["sid"]}""" + "}}, axis=0, fill_value=0)"
        return query



if __name__ == "__main__":
    tool = ShoojuTools()
    query = ""  # add your query here
    """DELETING FOLDERS"""
    tool.delete_sj_folder(query, )

    """Getting data from a sid into a csv"""
    sid = ""  # add your sid here
    date_start = "%Y-%m-%d"  # add your starting date here in the format of Year-Month-Day
    date_finish = ""  # add your starting date here in the format of Year-Month-Day
    tool.get_points_from_sid_into_df(sid=sid,
                                     date_finish=date_finish,
                                     date_start=date_start)

    """Creating queries where you add/subtract/divide/multiply individual sids from a folder"""
    query = ""
    operator_function = ""
    tool.add_sids_together(query, operator_function)
