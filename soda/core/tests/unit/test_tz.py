from datetime import datetime, timezone


def test_tz():
    print()
    t = datetime(2020, 6, 23, 0, 0, 10)
    print(datetime.fromtimestamp(t.timestamp()))
    print(datetime.utcfromtimestamp(t.timestamp()))

    t2 = datetime(2020, 6, 23, 0, 0, 10, tzinfo=timezone.utc)
    print(datetime.fromtimestamp(t2.timestamp()))
    print(datetime.utcfromtimestamp(t2.timestamp()))

    # print(datetime.fromtimestamp(datetime(2020, 6, 23, 0, 0, 10, tzinfo=timezone(offset=timedelta(hours=time.timezone))).timestamp()))
