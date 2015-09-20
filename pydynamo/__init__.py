from django.conf import settings
try:
    DYNAMO_SECRET_KEY = getattr(settings, "DYNAMO_SECRET_KEY")
    DYNAMO_ACCESS_KEY = getattr(settings, "DYNAMO_ACCESS_KEY")
except AttributeError, e:
    raise AttributeError(
        "Invalid configuration, Please set DYNAMO_SECRET_KEY,"
        " DYNAMO_ACCESS_KEY in project settings !")