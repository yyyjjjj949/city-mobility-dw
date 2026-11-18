import json
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from build_theme_analysis import active_files,hour_profile,ranked_od,safe_ratio,weighted_mean,weather_metrics,validate_day_hour

class ThemeAnalysisTests(unittest.TestCase):
    def test_average_is_reaggregated_from_weighted_denominators(self):
        data=pd.DataFrame({'seconds':[60.,9900.],'records':[1,99]})
        self.assertAlmostEqual(weighted_mean(data,'seconds','records',60),1.66)
        self.assertNotAlmostEqual(weighted_mean(data,'seconds','records',60),(1+100/60)/2)
        self.assertIsNone(safe_ratio(10,0))
    def test_complete_zero_day_stays_in_hourly_average(self):
        data=pd.DataFrame({'transport_code':['taxi_yellow','taxi_yellow'],'service_date':['2025-06-02','2025-06-03'],'hour_start_utc':['2025-06-02T16:00:00Z','2025-06-03T16:00:00Z'],'trip_count':[100,0],'source_complete':[True,True]})
        profile=hour_profile(data)
        self.assertEqual(profile.iloc[0].complete_days,2)
        self.assertEqual(profile.iloc[0].daily_average_trips,50)
    def test_incomplete_source_is_rejected(self):
        data=pd.DataFrame({'source_complete':[False]})
        with self.assertRaisesRegex(ValueError,'Incomplete'):hour_profile(data)
    def test_normalization_is_per_mode_not_all_modes(self):
        data=pd.DataFrame({'transport_code':['taxi_yellow','taxi_yellow','citibike','citibike'],'service_date':['2025-06-02']*4,'hour_start_utc':['2025-06-02T16:00:00Z','2025-06-02T17:00:00Z']*2,'trip_count':[90,10,1,9],'source_complete':[True]*4})
        p=hour_profile(data)
        self.assertTrue(p.groupby('transport_code').within_mode_share.sum().eq(1).all())
        self.assertAlmostEqual(p.loc[(p.transport_code=='citibike')&(p.local_hour==12)].iloc[0].within_mode_share,.1)
    def test_od_filter_first_and_full_denominator(self):
        data=pd.DataFrame([{'service_date':'2025-06-01','transport_code':'taxi_yellow','origin_zone_key':i,'destination_zone_key':i,'origin_zone_name':str(i),'destination_zone_name':str(i),'trip_count':10} for i in range(21)]+[{'service_date':'2025-06-02','transport_code':'taxi_yellow','origin_zone_key':0,'destination_zone_key':1,'origin_zone_name':'0','destination_zone_name':'1','trip_count':10000}])
        top,result=ranked_od(data,'taxi_yellow',['2025-06-01'])
        self.assertEqual(len(top),20)
        self.assertEqual(result['all_matched_od_trips'],210)
        self.assertAlmostEqual(result['top20_concentration'],200/210)
        self.assertEqual(top.iloc[0].origin_zone_key,0)
    def test_delta_replay_excludes_removed_files_and_later_versions(self):
        with tempfile.TemporaryDirectory() as folder:
            logs=Path(folder)/'_delta_log';logs.mkdir()
            (logs/'00000000000000000000.json').write_text(json.dumps({'add':{'path':'old.parquet'}}))
            (logs/'00000000000000000001.json').write_text(json.dumps({'remove':{'path':'old.parquet'}})+'\n'+json.dumps({'add':{'path':'current.parquet'}}))
            (logs/'00000000000000000002.json').write_text(json.dumps({'add':{'path':'later.parquet'}}))
            self.assertEqual(set(active_files(folder,1)),{'current.parquet'})
            with self.assertRaisesRegex(ValueError,'Missing'):active_files(folder,3)
    def test_weather_uses_each_mode_and_week_type_baseline(self):
        rows=[]
        for mode,base in [('taxi_yellow',100),('citibike',20)]:
            for weekday in [True,False]:
                scale=2 if weekday else 1
                for weather,amount,days in [('no_measurable',base*scale,5),('measurable',base*scale*.5,2)]:rows.append({'month_start':'2025-06-01','transport_code':mode,'is_weekday':weekday,'precipitation_group':weather,'complete_days':days,'daily_average_trips':amount})
        data=weather_metrics(pd.DataFrame(rows))
        self.assertTrue(data.loc[data.precipitation_group=='measurable'].recomputed_ratio.eq(.5).all())
        self.assertTrue(data.loc[data.precipitation_group=='measurable'].baseline_days.eq(5).all())
    def test_absent_or_zero_weather_baseline_is_unavailable(self):
        data=pd.DataFrame([{'month_start':'2025-06-01','transport_code':'taxi_yellow','is_weekday':True,'precipitation_group':'no_measurable','complete_days':2,'daily_average_trips':0},{'month_start':'2025-06-01','transport_code':'taxi_yellow','is_weekday':True,'precipitation_group':'measurable','complete_days':3,'daily_average_trips':20}])
        self.assertTrue(weather_metrics(data).recomputed_ratio.isna().all())

if __name__=='__main__':unittest.main()
