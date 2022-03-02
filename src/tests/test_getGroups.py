from contextlib import nullcontext
from lastfm.utils import apply_getGroups
from tests.utils.dataframe_test_utils import PySparkTestCase, test_schema, test_data
from pyspark.sql.types import StructType, StringType, LongType, IntegerType

class SimpleTestCase(PySparkTestCase):

    def test_getGroups_schema(self):
        input_schema = StructType() \
            .add("userid",StringType(),True) \
            .add("timestamp",StringType(),True) \
            .add("musicbrainz-artist-id",StringType(),True) \
            .add("artist-name",StringType(),True) \
            .add("musicbrainz-track-id",StringType(),True) \
            .add("track-name",StringType(),True) \
            .add("eventtime",LongType(),True)
        input_data = [("user_000066","2009-03-27T17:18:26Z","null","The Amen Corner","null","Our Love (Is In The Pocket)",1238174306),
            ("user_000066","2009-03-08T04:06:21Z","2ceb4e66-4eaa-4dba-ad3a-30df3b742557","Natalia Lafourcade","a8864539-e4cc-4fc7-93d5-59c3c6f19a64","En El 2000",1236485181),
            ("user_000066","2009-03-08T03:57:15Z","b539e453-c4fe-47e3-8a07-8517eac74429","宇多田ヒカル","7de5cd2c-cb9e-40e0-bdf7-768c757b3a35","Time Will Tell",1236484635),
            ("user_000066","2009-03-08T03:52:42Z","86443348-fd26-48cf-bd13-7ea7fa27505c","Crystal Kay","997cf64c-b1cf-4909-9c7d-752b47116141","あなたのそばで",1236484362),
            ("user_000066","2009-03-08T03:38:22Z","22a1161f-966c-4c38-aba0-c99cb59806de","Jyongri","46aacfaf-4e18-4a72-80d1-7d0a14a48e2d","Possession",1236483502),
            ("user_000066","2009-03-08T03:34:39Z","a0d2e581-7bb7-4755-ba42-79ee9f3b2244","Leah Dizon","null","Unmeisen (Album Version)",1236483279),
            ("user_000066","2009-03-08T03:27:50Z","b539e453-c4fe-47e3-8a07-8517eac74429","宇多田ヒカル","97324396-a586-4ccf-9b24-e64ad3359419","Final Distance",1236482870),
            ("user_000066","2009-03-07T23:41:12Z","39153756-5ffa-42a1-9b97-0468839021c3","Belanova","bd386035-36bc-476d-b2e9-6cd9cba6a792","Niño",1236469272),
            ("user_000066","2009-03-07T23:37:46Z","04cff564-a2e3-4560-8e7e-bc2539e7b46c","Reik","9dc118f9-c56e-45cc-8848-2a5005f83391","Que Vida La Mia",1236469066),
            ("user_000066","2009-03-07T23:34:03Z","04cff564-a2e3-4560-8e7e-bc2539e7b46c","Reik","17d73d1e-3a9b-4be0-8b7f-4016adc1d003","Me Duele Amarte",1236468843)]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        transformed_df = apply_getGroups(input_df)

        expected_schema = StructType() \
            .add("userid",StringType(),True) \
            .add("musicbrainz-track-id",StringType(),True) \
            .add("track-name",StringType(),True) \
            .add("timestamp",StringType(),True) \
            .add("eventtime",LongType(),True) \
            .add("songs_20_min",LongType(),False) \
            .add("is_new_session",IntegerType(),False) \
            .add("unique_id",LongType(),False) \
            .add("group_session_id",LongType(),True)

        expected_data = [("user_000066","null","Our Love (Is In The Pocket)","2009-03-27T17:18:26Z",1238174306,0,0,0,0),
	        ("user_000066","a8864539-e4cc-4fc7-93d5-59c3c6f19a64","En El 2000","2009-03-08T04:06:21Z",1236485181,1,1,1,1),
	        ("user_000066","7de5cd2c-cb9e-40e0-bdf7-768c757b3a35","Time Will Tell","2009-03-08T03:57:15Z",1236484635,2,0,2,1),
	        ("user_000066","997cf64c-b1cf-4909-9c7d-752b47116141","あなたのそばで","2009-03-08T03:52:42Z",1236484362,3,0,3,1),
	        ("user_000066","46aacfaf-4e18-4a72-80d1-7d0a14a48e2d","Possession","2009-03-08T03:38:22Z",1236483502,3,0,4,1),
	        ("user_000066","null","Unmeisen (Album Version)","2009-03-08T03:34:39Z",1236483279,2,0,5,1),
	        ("user_000066","97324396-a586-4ccf-9b24-e64ad3359419","Final Distance","2009-03-08T03:27:50Z",1236482870,2,0,6,1),
	        ("user_000066","bd386035-36bc-476d-b2e9-6cd9cba6a792","Niño","2009-03-07T23:41:12Z",1236469272,1,1,7,2),
	        ("user_000066","9dc118f9-c56e-45cc-8848-2a5005f83391","Que Vida La Mia","2009-03-07T23:37:46Z",1236469066,2,0,8,2),
	        ("user_000066","17d73d1e-3a9b-4be0-8b7f-4016adc1d003","Me Duele Amarte","2009-03-07T23:34:03Z",1236468843,3,0,9,2)]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        self.assertTrue(test_schema(transformed_df, expected_df))

    def test_getGroups_data(self):
        input_schema = StructType() \
            .add("userid",StringType(),True) \
            .add("timestamp",StringType(),True) \
            .add("musicbrainz-artist-id",StringType(),True) \
            .add("artist-name",StringType(),True) \
            .add("musicbrainz-track-id",StringType(),True) \
            .add("track-name",StringType(),True) \
            .add("eventtime",LongType(),True)
        input_data = [("user_000066","2009-03-27T17:18:26Z","null","The Amen Corner","null","Our Love (Is In The Pocket)",1238174306),
            ("user_000066","2009-03-08T04:06:21Z","2ceb4e66-4eaa-4dba-ad3a-30df3b742557","Natalia Lafourcade","a8864539-e4cc-4fc7-93d5-59c3c6f19a64","En El 2000",1236485181),
            ("user_000066","2009-03-08T03:57:15Z","b539e453-c4fe-47e3-8a07-8517eac74429","宇多田ヒカル","7de5cd2c-cb9e-40e0-bdf7-768c757b3a35","Time Will Tell",1236484635),
            ("user_000066","2009-03-08T03:52:42Z","86443348-fd26-48cf-bd13-7ea7fa27505c","Crystal Kay","997cf64c-b1cf-4909-9c7d-752b47116141","あなたのそばで",1236484362),
            ("user_000066","2009-03-08T03:38:22Z","22a1161f-966c-4c38-aba0-c99cb59806de","Jyongri","46aacfaf-4e18-4a72-80d1-7d0a14a48e2d","Possession",1236483502),
            ("user_000066","2009-03-08T03:34:39Z","a0d2e581-7bb7-4755-ba42-79ee9f3b2244","Leah Dizon","null","Unmeisen (Album Version)",1236483279),
            ("user_000066","2009-03-08T03:27:50Z","b539e453-c4fe-47e3-8a07-8517eac74429","宇多田ヒカル","97324396-a586-4ccf-9b24-e64ad3359419","Final Distance",1236482870),
            ("user_000066","2009-03-07T23:41:12Z","39153756-5ffa-42a1-9b97-0468839021c3","Belanova","bd386035-36bc-476d-b2e9-6cd9cba6a792","Niño",1236469272),
            ("user_000066","2009-03-07T23:37:46Z","04cff564-a2e3-4560-8e7e-bc2539e7b46c","Reik","9dc118f9-c56e-45cc-8848-2a5005f83391","Que Vida La Mia",1236469066),
            ("user_000066","2009-03-07T23:34:03Z","04cff564-a2e3-4560-8e7e-bc2539e7b46c","Reik","17d73d1e-3a9b-4be0-8b7f-4016adc1d003","Me Duele Amarte",1236468843)]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        transformed_df = apply_getGroups(input_df)

        expected_schema = StructType() \
            .add("userid",StringType(),True) \
            .add("musicbrainz-track-id",StringType(),True) \
            .add("track-name",StringType(),True) \
            .add("timestamp",StringType(),True) \
            .add("eventtime",LongType(),True) \
            .add("songs_20_min",LongType(),False) \
            .add("is_new_session",IntegerType(),False) \
            .add("unique_id",LongType(),False) \
            .add("group_session_id",LongType(),True)

        expected_data = [("user_000066","null","Our Love (Is In The Pocket)","2009-03-27T17:18:26Z",1238174306,0,0,0,0),
	        ("user_000066","a8864539-e4cc-4fc7-93d5-59c3c6f19a64","En El 2000","2009-03-08T04:06:21Z",1236485181,1,1,1,1),
	        ("user_000066","7de5cd2c-cb9e-40e0-bdf7-768c757b3a35","Time Will Tell","2009-03-08T03:57:15Z",1236484635,2,0,2,1),
	        ("user_000066","997cf64c-b1cf-4909-9c7d-752b47116141","あなたのそばで","2009-03-08T03:52:42Z",1236484362,3,0,3,1),
	        ("user_000066","46aacfaf-4e18-4a72-80d1-7d0a14a48e2d","Possession","2009-03-08T03:38:22Z",1236483502,3,0,4,1),
	        ("user_000066","null","Unmeisen (Album Version)","2009-03-08T03:34:39Z",1236483279,2,0,5,1),
	        ("user_000066","97324396-a586-4ccf-9b24-e64ad3359419","Final Distance","2009-03-08T03:27:50Z",1236482870,2,0,6,1),
	        ("user_000066","bd386035-36bc-476d-b2e9-6cd9cba6a792","Niño","2009-03-07T23:41:12Z",1236469272,1,1,7,2),
	        ("user_000066","9dc118f9-c56e-45cc-8848-2a5005f83391","Que Vida La Mia","2009-03-07T23:37:46Z",1236469066,2,0,8,2),
	        ("user_000066","17d73d1e-3a9b-4be0-8b7f-4016adc1d003","Me Duele Amarte","2009-03-07T23:34:03Z",1236468843,3,0,9,2)]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        self.assertTrue(test_data(transformed_df, expected_df))