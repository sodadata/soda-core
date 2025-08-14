import freezegun
from helpers.test_fixtures import *  # noqa: F401
from soda_core.common.logging_configuration import configure_logging


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)


def monkey_patch_freezegun_for_google_api():
    def utcnow():
        if freezegun.api._should_use_real_time():
            result = freezegun.api.real_datetime.utcnow()
        else:
            result = freezegun.api.FakeDatetime._time_to_freeze()
        return freezegun.api.datetime_to_fakedatetime(result)

    def now(tz=None):
        if freezegun.api._should_use_real_time():
            result = freezegun.api.real_datetime.now(tz=tz)
        else:
            result = freezegun.api.FakeDatetime._time_to_freeze()
        return freezegun.api.datetime_to_fakedatetime(result)

    setattr(freezegun.api.FakeDatetime, "utcnow", utcnow)
    setattr(freezegun.api.FakeDatetime, "now", now)


monkey_patch_freezegun_for_google_api()


my_ignore_list = [
    "pyathena",
    "boto3",
    "botocore",
    "boto3",
    "botocore",
    "botocore.auth",
    "botocore.credentials",
    "botocore.awsrequest",
]
freezegun.configure(extend_ignore_list=my_ignore_list)
freeze_time = freezegun.freeze_time
