from shooju import Connection, Point
import os
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, date, timedelta
import shooju
import re
import pandas as pd

load_dotenv(find_dotenv())


class ShoojuTools:
    def __init__(self):
        try:
            self.shooju_api_key = os.getenv("SHOOJU_API_KEY")
            self.shooju_api_user_name = os.getenv("SHOOJU_USER_NAME")
        except Exception as e:
            raise e
        self.sj = Connection(server='https://energyaspects.shooju.com/',
                             user=self.shooju_api_user_name,
                             api_key=self.shooju_api_key)

    def get_points_from_sid_into_df(self, sid, date_start, date_finish, number_of_points=-1):
        data = self.sj.get_points(series_id=sid,
                                  date_start=date_start,
                                  date_finish=date_finish,
                                  max_points=number_of_points,
                                  serializer=shooju.pd_series).sort_index()
        return data


class EuSumming(ShoojuTools):
    sid_request = r"((GIE\AGSI\Storage\ByFacility\*)) AND (source_obj.attribute=gasInStorage || " \
                  r"NOT set=source_obj.attribute)"
    date_start = "MAX"
    date_finish = "MIN"
    number_of_points = -1

    def __init__(self):
        ShoojuTools.__init__(self)
        self.scroller = self.sj.scroll(self.sid_request,
                                       fields=['sid'],
                                       max_points=-1,
                                       serializer=shooju.points_serializers.pd_series)
        list = []
        for series in self.scroller:
            list.append(self.get_points_from_sid_into_df(sid=series['fields']['sid'],
                                                         date_start=self.date_start,
                                                         date_finish=self.date_finish,
                                                         number_of_points=self.number_of_points)
                        )
        self.df = pd.concat(list)
        self.metadata = {'source_obj.attribute': "gasInStorage",
                         'source_obj.units': "TWH gasInStorage",
                         'source_obj.region_name': "Europe",
                         'source_obj.facility_slug': "Total Europe",
                         "source_obj.country_name": "Total Europe",
                         'source_obj.description': "GIE AGSI Storage for the entire Europe Gas in Storage",
                         }


if '__main__' == __name__:
    tool = EuSumming()
    series = tool.df.groupby(tool.df.index).sum()
    sid = r"users\constantinos.spanachis\AGSI\total_europe"
    job_description = "Test Creating AGsi Total Europe"
    try:
        with tool.sj.register_job(job_description, batch_size=10000) as job:
            job.put_fields(series_id=sid,
                           fields=tool.metadata)
            job.put_points(series_id=sid,
                           points=[Point(idx, value) for idx, value in series.to_dict().items()])
    except Exception as error:
        raise error
